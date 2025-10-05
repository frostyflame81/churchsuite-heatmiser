import asyncio
import datetime
import json
import logging
import time
import requests # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler # type: ignore
import argparse
import os
import pytz # type: ignore
import dateutil.parser # type: ignore
from typing import Dict, Any, List, Optional
import websockets # type: ignore
import ssl
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError, WebSocketClient # type: ignore

# Configuration
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
PREHEAT_TIME_MINUTES = int(os.environ.get("PREHEAT_TIME_MINUTES", 30))
DEFAULT_TEMPERATURE = float(os.environ.get("DEFAULT_TEMPERATURE", 19))
ECO_TEMPERATURE = float(os.environ.get("ECO_TEMPERATURE", 12))
TEMPERATURE_SENSITIVITY = int(os.environ.get("TEMPERATURE_SENSITIVITY", 10))
PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE = float(
    os.environ.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 5)
)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()
CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOGGING_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Global store for neohub clients
neohubs: Dict[str, NeoHub] = {}
config: Dict[str, Any] = {}

# --- Utility and Configuration Functions ---

def load_config() -> Optional[Dict[str, Any]]:
    """Loads configuration from the specified JSON file."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {CONFIG_FILE}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in configuration file: {CONFIG_FILE}")
        return None

def validate_config(config_data: Dict[str, Any]) -> bool:
    """Validates the structure of the loaded configuration."""
    if "neohubs" not in config_data or not isinstance(config_data["neohubs"], dict):
        logging.error("Config missing 'neohubs' section or it is not a dictionary.")
        return False
    if "locations" not in config_data or not isinstance(config_data["locations"], dict):
        logging.error("Config missing 'locations' section or it is not a dictionary.")
        return False
    return True

# --- Neohub Connection and Control Functions ---

def connect_to_neohub(name: str, neohub_config: Dict[str, Any]) -> bool:
    """Initializes and connects to a NeoHub client."""
    host = neohub_config.get("address")
    port = neohub_config.get("port", 4242)
    token = neohub_config.get("token")

    if not host or not token:
        logging.error(f"Neohub '{name}' is missing address or token in config.")
        return False

    try:
        hub = NeoHub(host=host, port=port, token=token)
        neohubs[name] = hub
        logging.info(f"Initialized NeoHub client for '{name}' at {host}:{port}")
        return True
    except Exception as e:
        logging.error(f"Error initializing NeoHub for '{name}': {e}")
        return False

async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Fetches the list of zone names from a connected NeoHub."""
    hub = neohubs.get(neohub_name)
    if not hub:
        logging.error(f"Neohub '{neohub_name}' is not connected.")
        return None
    try:
        await hub.connect()
        devices = await hub.get_devices()
        await hub.disconnect() # Disconnect after fetching
        return [dev.name for dev in devices]
    except (NeoHubConnectionError, asyncio.TimeoutError) as e:
        logging.error(f"Error fetching zones from '{neohub_name}': {e}")
        return None

# --- Weather and External Data Functions ---

def get_external_temperature() -> Optional[float]:
    """Fetches the current external temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY or not OPENWEATHERMAP_CITY:
        logging.warning("OpenWeatherMap API key or city not configured. Skipping external temperature fetch.")
        return None

    url = (
        f"http://api.openweathermap.org/data/2.5/weather?"
        f"q={OPENWEATHERMAP_CITY}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        temp = data["main"]["temp"]
        logging.info(f"External temperature for {OPENWEATHERMAP_CITY}: {temp}°C")
        return temp
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching external temperature: {e}")
        return None

def get_churchsuite_events() -> List[Dict[str, Any]]:
    """Fetches events from ChurchSuite URL and returns a list of event dictionaries."""
    if not CHURCHSUITE_URL:
        logging.warning("ChurchSuite URL not configured. Skipping event fetch.")
        return []

    try:
        response = requests.get(CHURCHSUITE_URL, timeout=15)
        response.raise_for_status()
        
        # ChurchSuite sometimes wraps the JSON data in a list or has extra keys.
        data = response.json()
        
        if isinstance(data, dict):
            # Try to find the list of events, assuming a key like 'data' or 'events'
            events_list = data.get('data', data.get('events', []))
        elif isinstance(data, list):
            events_list = data
        else:
            logging.error(f"Unexpected ChurchSuite response format: {type(data)}")
            return []

        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"Raw events fetched: {len(events_list)}")
            
        return events_list

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching ChurchSuite events: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding ChurchSuite JSON response: {e}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during ChurchSuite event fetch: {e}")
        return []

# --- Heating Schedule Calculation Logic ---

def calculate_schedule(
    events: List[Any],
    location_name: str,
    preheat_time_minutes: int,
    default_temperature: float,
    eco_temperature: float,
    external_temp: Optional[float],
    heat_loss_factor: float,
    min_external_temp: float,
    adjustment_per_degree: float,
    timezone_str: str,
) -> List[Dict[str, Any]]:
    """
    Calculates the heating schedule profile for a specific location based on events,
    external temperature, and preheat settings.
    """
    profile: List[Dict[str, Any]] = []
    
    local_tz = pytz.timezone(timezone_str)
    now = datetime.datetime.now(local_tz)

    # Filter for relevant events for this location and clean up non-dictionary entries
    # FIX: Added 'isinstance(event, dict)' check to prevent AttributeError when 'event' is a string.
    relevant_events = [
        event for event in events
        if isinstance(event, dict) and location_name in event.get("rooms", []) and
        dateutil.parser.parse(event.get("end_date", now.isoformat())).astimezone(local_tz) >= now
    ]
    
    if not relevant_events:
        logging.info(f"No future events found for location: {location_name}. Setting to ECO temp.")
        return [] 

    # Sort events by start time
    relevant_events.sort(key=lambda event: dateutil.parser.parse(event.get("start_date", now.isoformat())))

    # Determine preheat adjustment based on external temperature
    preheat_adjustment = 0
    if external_temp is not None and external_temp < min_external_temp:
        temp_difference = min_external_temp - external_temp
        # Calculate adjustment based on difference, factor, and minutes/degree
        preheat_adjustment = int(
            temp_difference * heat_loss_factor * adjustment_per_degree
        )
        logging.info(
            f"External temp ({external_temp}°C) is below minimum ({min_external_temp}°C). "
            f"Adding {preheat_adjustment} minutes preheat adjustment for '{location_name}'."
        )

    # Process events to create the profile
    for event in relevant_events:
        try:
            start_dt_utc = dateutil.parser.parse(event["start_date"])
            end_dt_utc = dateutil.parser.parse(event["end_date"])

            # Convert to local timezone for processing
            local_tz_dt = pytz.timezone(os.environ.get("CHURCHSUITE_TIMEZONE", "UTC"))
            start_dt_local = start_dt_utc.astimezone(local_tz_dt)
            end_dt_local = end_dt_utc.astimezone(local_tz_dt)

            # Calculate the time to start heating (preheat time before the event starts)
            preheat_start_dt = start_dt_local - datetime.timedelta(
                minutes=preheat_time_minutes + preheat_adjustment
            )

            # Schedule the heating to come ON at the preheat start time
            profile.append(
                {
                    "type": "ON",
                    "time": preheat_start_dt,
                    "target_temp": DEFAULT_TEMPERATURE,
                    "event_name": event.get("name", "Unnamed Event"),
                }
            )

            # Schedule the heating to go OFF (back to ECO) after the event ends
            profile.append(
                {
                    "type": "OFF",
                    "time": end_dt_local,
                    "target_temp": ECO_TEMPERATURE, # Ensure it drops back to ECO
                    "event_name": event.get("name", "Unnamed Event"),
                }
            )
        except (KeyError, ValueError, TypeError) as e:
            logging.error(f"Error processing event for '{location_name}': {event}. Error: {e}")
            continue

    # Clean up and combine profile entries
    # 1. Sort by time
    profile.sort(key=lambda p: p["time"])

    # Convert datetime objects to string format for Neohub (e.g., ISO format)
    final_profile = [
        {
            "time": p["time"].isoformat(),
            "target_temp": p["target_temp"],
            "event_name": p["event_name"],
            "type": p["type"],
        }
        for p in profile
    ]

    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Calculated profile for {location_name}: {json.dumps(final_profile, indent=2)}")

    return final_profile

# --- Neohub API Application Functions ---

async def apply_schedule_to_heating(
    neohub_name: str, zone_names: List[str], profile_data: List[Dict[str, Any]]
) -> None:
    """Applies the calculated heating schedule to the specified NeoHub zones."""
    hub = neohubs.get(neohub_name)
    if not hub:
        logging.error(f"Neohub '{neohub_name}' is not connected for schedule application.")
        return

    try:
        await hub.connect()
        # Find all zones that match the zone_names
        all_devices = await hub.get_devices()
        target_zones = [
            dev for dev in all_devices if dev.name in zone_names
        ]

        if not target_zones:
            logging.warning(
                f"No matching zones found on '{neohub_name}' for names: {zone_names}"
            )
            return

        logging.info(
            f"Applying schedule to zones on '{neohub_name}': {[z.name for z in target_zones]}"
        )

        for zone in target_zones:
            # We are using the profile data to determine the next ON/OFF time.
            # Find the next heating ON event in the profile
            next_on_event = None
            
            # The original code did not define CHURCHSUITE_TIMEZONE globally,
            # but it is set in env variables. We'll grab it here for safety, 
            # or assume the `timezone_str` passed to calculate_schedule is sufficient.
            # However, for applying, we need the current timezone.
            timezone_str = os.environ.get("CHURCHSUITE_TIMEZONE", "UTC")
            now_local = datetime.datetime.now(pytz.timezone(timezone_str))
            
            for event in profile_data:
                # The 'time' field is an ISO-formatted string from calculate_schedule
                event_time = dateutil.parser.parse(event["time"]).astimezone(pytz.timezone(timezone_str))
                
                if event_time > now_local and event["type"] == "ON":
                    next_on_event = event
                    break
            
            if next_on_event:
                # Calculate duration in minutes for the 'HOLD' command
                
                # Find the next OFF event that follows the next ON event
                next_off_event = None
                on_event_time = dateutil.parser.parse(next_on_event["time"]).astimezone(pytz.timezone(timezone_str))
                
                # Find the corresponding OFF time for this ON time.
                for event in profile_data:
                    event_time = dateutil.parser.parse(event["time"]).astimezone(pytz.timezone(timezone_str))
                    
                    # Look for the first OFF event strictly after the ON time
                    if event_time > on_event_time and event["type"] == "OFF":
                        next_off_event = event
                        break

                if next_off_event:
                    off_event_time = dateutil.parser.parse(next_off_event["time"]).astimezone(pytz.timezone(timezone_str))
                    
                    # Duration from now until the OFF time in minutes
                    duration_td = off_event_time - now_local
                    duration_minutes = int(duration_td.total_seconds() / 60)

                    if duration_minutes > 0:
                        # Schedule to HOLD until the OFF time at the target temperature
                        target_temp = next_on_event["target_temp"]
                        
                        # Apply the hold command
                        logging.info(
                            f"Zone '{zone.name}' schedule ON: HOLD at {target_temp}°C for {duration_minutes} mins (until {off_event_time.strftime('%H:%M')})"
                        )
                        await hub.set_hold(zone.name, target_temp, duration_minutes)
                    else:
                        logging.info(f"Zone '{zone.name}' next ON event time is in the past or duration is zero. Skipping hold.")
                
            # If no future ON event is found, set the temperature to ECO
            if not next_on_event:
                logging.info(f"Zone '{zone.name}' has no future ON events. Setting to ECO temperature ({ECO_TEMPERATURE}°C).")
                # Set a hold for 7 days (10080 minutes) at ECO, assuming schedule is not used/set.
                await hub.set_hold(zone.name, ECO_TEMPERATURE, 10080) 
                
        await hub.disconnect()

    except (NeoHubConnectionError, asyncio.TimeoutError) as e:
        logging.error(f"Error applying schedule to '{neohub_name}': {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred in apply_schedule_to_heating for '{neohub_name}': {e}")
    
async def update_heating_schedule():
    """Main function to orchestrate fetching data and applying heating schedules."""
    logging.info("--- Starting heating schedule update ---")
    
    # Define CHURCHSUITE_TIMEZONE locally for safety since it's an env var
    CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "UTC")

    # 1. Fetch external data
    external_temp = get_external_temperature()
    events = get_churchsuite_events()
    
    # 2. Iterate through all configured locations
    for location_name, loc_config in config["locations"].items():
        try:
            neohub_name = loc_config["neohub"]
            zone_names = loc_config["zones"]
            heat_loss_factor = loc_config["heat_loss_factor"]
            min_external_temp = loc_config["min_external_temp"]

            # 3. Calculate the new schedule profile
            profile_data = calculate_schedule(
                events=events,
                location_name=location_name,
                preheat_time_minutes=PREHEAT_TIME_MINUTES,
                default_temperature=DEFAULT_TEMPERATURE,
                eco_temperature=ECO_TEMPERATURE,
                external_temp=external_temp,
                heat_loss_factor=heat_loss_factor,
                min_external_temp=min_external_temp,
                adjustment_per_degree=PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE,
                timezone_str=CHURCHSUITE_TIMEZONE, # Pass timezone for safety
            )

            # 4. Apply the profile to the NeoHub zones
            await apply_schedule_to_heating(
                neohub_name=neohub_name,
                zone_names=zone_names,
                profile_data=profile_data,
            )

        except KeyError as e:
            logging.error(f"Missing key in configuration for location '{location_name}': {e}")
            continue
        except Exception as e:
            logging.exception(f"Unhandled error processing location '{location_name}': {e}")
            continue

    logging.info("--- Heating schedule update finished ---")


# --- Main Execution ---

def main():
    """The main entry point of the application."""
    global config
    
    # Load and validate configuration
    config_data = load_config()
    if config_data is None:
        logging.error("Failed to load configuration. Exiting.")
        return
    config = config_data # Store globally

    # This check needs to be placed after config is loaded but before it's used to connect.
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit()

    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # Initialize NeoHub connections
    for neohub_name, neohub_config in config["neohubs"].items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()
            
    # Optional: Fetch and log zones (for debug/info)
    for neohub_name in config["neohubs"]:
        try:
            zones = asyncio.run(get_zones(neohub_name))
            if zones:
                logging.info(f"Zones on {neohub_name}: {zones}")
                if LOGGING_LEVEL == "DEBUG":
                    logging.debug(f"main: Zones on {neohub_name}: {zones}")
        except Exception as e:
             logging.error(f"Error getting zones for {neohub_name}: {e}")


    # Create a scheduler.
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    asyncio.run(update_heating_schedule())  # Run immediately
        
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()

    try:
        while True:
            time.sleep(600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
