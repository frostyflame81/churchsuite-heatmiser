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
CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
PREHEAT_TIME_MINUTES = int(os.environ.get("PREHEAT_TIME_MINUTES", 30))
DEFAULT_TEMPERATURE = float(os.environ.get("DEFAULT_TEMPERATURE", 19))
ECO_TEMPERATURE = float(os.environ.get("ECO_TEMPERATURE", 12))
TEMPERATURE_SENSITIVITY = int(os.environ.get("TEMPERATURE_SENSITIVITY", 10))
PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE = float(
    os.environ.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 5)
)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOGGING_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Global store for NeoHub objects and config
NEOHUBS: Dict[str, NeoHub] = {}
config: Dict[str, Any] = {}

def load_config(config_file: str) -> Optional[Dict[str, Any]]:
    """Loads the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_file}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from {config_file}")
        return None

def connect_to_neohub(name: str, neohub_config: Dict[str, Any]) -> bool:
    """Connects to a single NeoHub and stores the object globally."""
    address = neohub_config.get("address")
    port = neohub_config.get("port")
    token = neohub_config.get("token")
    
    if not all([address, port, token]):
        logging.error(f"Missing address, port, or token for NeoHub: {name}")
        return False
        
    try:
        neohub = NeoHub(address, port, token)
        # Attempt a connection test (can be a quick property access)
        # We don't need to explicitly call connect() but the initial NeoHub instantiation might trigger a check.
        # For simplicity, we just store it. Connection issues will arise in later async calls.
        NEOHUBS[name] = neohub
        logging.info(f"NeoHub '{name}' initialized for connection.")
        return True
    except NeoHubUsageError as e:
        logging.error(f"NeoHub usage error for {name}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error initializing NeoHub {name}: {e}")
        return False

async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Retrieves zones from a specific NeoHub."""
    neohub = NEOHUBS.get(neohub_name)
    if not neohub:
        logging.warning(f"NeoHub '{neohub_name}' is not connected.")
        return None
    
    try:
        zones = await neohub.get_zones()
        # The zones list is extracted from the response object
        return [zone['name'] for zone in zones]
    except NeoHubConnectionError as e:
        logging.error(f"Connection error to NeoHub '{neohub_name}' during zone retrieval: {e}")
        return None
    except Exception as e:
        logging.error(f"Error getting zones from NeoHub '{neohub_name}': {e}")
        return None

def validate_config(cfg: Dict[str, Any]) -> bool:
    """Validates configuration keys."""
    if "neohubs" not in cfg or not isinstance(cfg["neohubs"], dict):
        logging.error("Config validation failed: 'neohubs' key is missing or invalid.")
        return False
    if "locations" not in cfg or not isinstance(cfg["locations"], dict):
        logging.error("Config validation failed: 'locations' key is missing or invalid.")
        return False
    
    for loc_name, loc_config in cfg["locations"].items():
        if "neohub" not in loc_config or loc_config["neohub"] not in cfg["neohubs"]:
            logging.error(f"Config validation failed: Location '{loc_name}' refers to an unknown 'neohub'.")
            return False
        if "zones" not in loc_config or not isinstance(loc_config["zones"], list):
            logging.error(f"Config validation failed: Location '{loc_name}' is missing or has invalid 'zones'.")
            return False
        # Minimal check for required heating parameters
        if "heat_loss_factor" not in loc_config or "min_external_temp" not in loc_config:
             logging.error(f"Config validation failed: Location '{loc_name}' is missing required heating parameters.")
             return False

    return True

def get_external_temperature(city: str, api_key: str) -> Optional[float]:
    """Fetches the current external temperature using OpenWeatherMap."""
    try:
        url = (
            f"http://api.openweathermap.org/data/2.5/weather?"
            f"q={city}&appid={api_key}&units=metric"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        current_temp = data["main"]["temp"]
        logging.info(f"Current external temperature in {city}: {current_temp}°C")
        return current_temp
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None
    except KeyError:
        logging.error("Weather data response missing 'main' or 'temp' keys.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during weather fetch: {e}")
        return None

def calculate_preheat_minutes(
    target_temp: float,
    current_temp: float,
    heat_loss_factor: float,
    external_temp: float,
) -> int:
    """
    Calculates the required pre-heat time based on internal, external temps,
    and a heat loss factor.

    This implements a basic model:
    Preheat Time = BASE_PREHEAT + (Heat Loss Factor * Temp Differential * Adjustment per Degree)
    """
    
    # 1. Base Preheat Time (e.g., the standard 30 minutes)
    base_preheat = PREHEAT_TIME_MINUTES
    
    # 2. Temperature Differential (How much heat is lost to the outside)
    # Use a minimum of 0 to avoid negative preheat times if it's hot outside
    temp_differential = max(0.0, target_temp - external_temp)
    
    # 3. Adjustment based on differential and heat loss
    adjustment = (
        heat_loss_factor * temp_differential * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE
    )
    
    total_preheat_minutes = int(round(base_preheat + adjustment))
    
    # Ensure a sensible minimum and maximum
    min_preheat = 15
    max_preheat = 180
    
    final_preheat = max(min_preheat, min(max_preheat, total_preheat_minutes))
    
    logging.debug(
        f"Preheat calculation: Base={base_preheat}, "
        f"Differential={temp_differential:.1f}, "
        f"Adjustment={adjustment:.1f}, "
        f"Final={final_preheat} minutes."
    )
    
    return final_preheat


async def update_neohub_zone(neohub_name: str, zone_name: str, temp: float) -> bool:
    """Sets a new temperature target for a specific zone on a NeoHub."""
    neohub = NEOHUBS.get(neohub_name)
    if not neohub:
        logging.error(f"NeoHub '{neohub_name}' not found for zone {zone_name}.")
        return False
        
    try:
        logging.info(f"Setting {zone_name} on {neohub_name} to {temp}°C.")
        
        # Check current temperature to avoid setting if already at or above target (within sensitivity)
        current_zones = await neohub.get_zones()
        current_zone_info = next((z for z in current_zones if z.get('name') == zone_name), None)

        if current_zone_info:
            current_temp = current_zone_info.get('temp')
            if current_temp is not None and current_temp >= temp - (TEMPERATURE_SENSITIVITY / 100):
                 logging.info(
                    f"Zone {zone_name} is already at {current_temp}°C. "
                    f"Skipping temperature set as it meets the target {temp}°C (sensitivity applied)."
                )
                 return True

        # Set the temperature
        await neohub.set_zone_temp(zone_name, temp)
        logging.info(f"Successfully set {zone_name} on {neohub_name} to {temp}°C.")
        return True
    except NeoHubConnectionError as e:
        logging.error(f"Connection error setting temp for {zone_name} on {neohub_name}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error setting temp for {zone_name} on {neohub_name}: {e}")
        return False

async def update_heating_schedule():
    """Main function to fetch bookings, calculate schedules, and update NeoHubs."""
    logging.info("Starting heating schedule update...")
    
    # 1. Fetch external temperature
    external_temp = get_external_temperature(
        OPENWEATHERMAP_CITY, OPENWEATHERMAP_API_KEY
    )
    
    if external_temp is None:
        logging.error("Failed to get external temperature. Aborting update.")
        return

    # 2. Fetch ChurchSuite bookings
    try:
        response = requests.get(CHURCHSUITE_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching ChurchSuite data: {e}")
        return
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from ChurchSuite response.")
        return
    
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Raw ChurchSuite Data: {data}")

    # --- THE CRITICAL FIX IS HERE ---
    # Use 'booked_resources' and 'resources' based on the confirmed API structure
    bookings = data.get("booked_resources", []) 
    resource_map = data.get("resources", {})
    # --- END CRITICAL FIX ---

    if not bookings and not resource_map:
        logging.warning("No bookings or resource map found in data. Aborting update.")
        return

    # Map resource IDs to location names for easier lookups
    location_map: Dict[str, str] = {}
    for resource_id, resource_info in resource_map.items():
        # resource_info['name'] contains the resource name, which should match the keys in config['locations']
        # The key in resource_map is the resource_id (e.g. '30')
        location_map[resource_id] = resource_info.get("name", "")

    # 3. Determine required target times and temperatures
    target_schedules: Dict[str, Dict[str, Any]] = {}
    current_time_utc = datetime.datetime.now(pytz.utc)
    
    try:
        local_timezone = pytz.timezone(CHURCHSUITE_TIMEZONE)
    except pytz.UnknownTimeZoneError:
        logging.error(f"Unknown timezone: {CHURCHSUITE_TIMEZONE}. Using UTC.")
        local_timezone = pytz.utc
        
    current_time_local = current_time_utc.astimezone(local_timezone)
    
    # Define the end of the day to limit the scope of bookings (e.g., next 24 hours)
    search_end_time = current_time_local + datetime.timedelta(hours=24) 

    for booking in bookings:
        resource_id = str(booking.get("resource_id"))
        resource_name = location_map.get(resource_id)

        # Skip if resource name is unknown or not configured
        if not resource_name or resource_name not in config["locations"]:
            logging.debug(f"Skipping booking for unconfigured resource ID: {resource_id}")
            continue

        # Parse booking times
        try:
            start_dt_utc = dateutil.parser.parse(booking["start_datetime"]).astimezone(pytz.utc)
            end_dt_utc = dateutil.parser.parse(booking["end_datetime"]).astimezone(pytz.utc)
            
            # Convert to local time for time comparison
            start_dt_local = start_dt_utc.astimezone(local_timezone)
            end_dt_local = end_dt_utc.astimezone(local_timezone)

        except (KeyError, dateutil.parser.ParserError) as e:
            logging.error(f"Skipping booking due to time parsing error: {e} in {booking}")
            continue

        # Only process bookings starting soon
        if start_dt_local > search_end_time:
            continue
            
        # Also skip bookings that have already finished (with a small buffer)
        if end_dt_local < current_time_local - datetime.timedelta(minutes=15):
            continue

        loc_config = config["locations"][resource_name]
        
        # Calculate the required pre-heat time
        # This calculation needs the current internal temperature which we don't have easily.
        # For simplicity, we can assume the internal starting temp is a few degrees above the eco temp, 
        # or just use a fixed value like 15°C if the neoHubs don't expose current room temp reliably/quickly.
        # Since we cannot easily and quickly get the current internal temperature of all zones, 
        # we will use a conservative guess for 'current_internal_temp' in the calculation,
        # or better yet, just omit it and focus on the external differential.
        # Let's use a simplified model based on external temp for now.
        
        # NOTE: A true implementation would fetch the *actual* current room temp for that zone
        # using await neohub.get_zones(), which is slow for a loop.
        
        # Target internal temperature (use DEFAULT_TEMPERATURE for all bookings)
        target_temp = DEFAULT_TEMPERATURE
        
        # Calculate preheat based on external temp and heat loss factor
        preheat_minutes = PREHEAT_TIME_MINUTES # Fallback
        
        # Check if external temperature is below the minimum external temp threshold for this location
        min_temp_threshold = loc_config.get("min_external_temp", 5)
        if external_temp < min_temp_threshold:
            # Only calculate complex pre-heat if it's cold enough to matter
            preheat_minutes = calculate_preheat_minutes(
                target_temp=target_temp,
                # Current internal temp is hard to get fast, so we estimate a cold starting point
                current_temp=ECO_TEMPERATURE, 
                heat_loss_factor=loc_config.get("heat_loss_factor", 1.0),
                external_temp=external_temp,
            )

        preheat_start_dt_local = start_dt_local - datetime.timedelta(minutes=preheat_minutes)
        
        # Check if the calculated pre-heat start is within the current window
        # We want to turn on the heat if:
        # 1. The preheat start time is in the past OR
        # 2. The preheat start time is in the very near future (e.g., up to 30 mins from now)
        is_active_now = preheat_start_dt_local <= current_time_local and end_dt_local > current_time_local

        if is_active_now:
            if resource_name not in target_schedules:
                target_schedules[resource_name] = {
                    "start_time_local": preheat_start_dt_local,
                    "end_time_local": end_dt_local,
                    "target_temp": target_temp,
                    "zones": loc_config["zones"],
                    "neohub": loc_config["neohub"],
                }
            else:
                # Merge overlapping bookings: take the earliest preheat start and the latest end time
                current_schedule = target_schedules[resource_name]
                current_schedule["start_time_local"] = min(
                    current_schedule["start_time_local"], preheat_start_dt_local
                )
                current_schedule["end_time_local"] = max(
                    current_schedule["end_time_local"], end_dt_local
                )
                # Keep the highest temperature if different bookings require different settings
                current_schedule["target_temp"] = max(
                    current_schedule["target_temp"], target_temp
                )
            
            logging.debug(
                f"Active booking for {resource_name}: {start_dt_local.strftime('%H:%M')} - {end_dt_local.strftime('%H:%M')} "
                f"(Preheat starts: {preheat_start_dt_local.strftime('%H:%M')}) at {target_temp}°C."
            )

    # 4. Apply schedules to NeoHub zones
    all_neohub_zones: Dict[str, List[str]] = {}
    for hub_name in config["neohubs"].keys():
        all_neohub_zones[hub_name] = []

    # Compile all zones that should be ON
    zones_to_heat: Dict[str, Dict[str, float]] = {} # {neohohub_name: {zone_name: target_temp}}

    for resource_name, schedule in target_schedules.items():
        hub_name = schedule["neohub"]
        temp = schedule["target_temp"]
        for zone in schedule["zones"]:
            # Check if this zone is already targeted by a different resource/schedule
            current_target = zones_to_heat.setdefault(hub_name, {}).get(zone)
            if current_target is None or temp > current_target:
                # Set or update with the higher required temperature
                zones_to_heat[hub_name][zone] = temp
                
    # Compile a list of all known zones to find those that should be OFF
    for resource_name, loc_config in config["locations"].items():
        hub_name = loc_config["neohub"]
        for zone in loc_config["zones"]:
             if zone not in all_neohub_zones[hub_name]:
                 all_neohub_zones[hub_name].append(zone)

    # Apply ON schedules
    for hub_name, zone_temps in zones_to_heat.items():
        for zone_name, temp in zone_temps.items():
            await update_neohub_zone(hub_name, zone_name, temp)

    # Apply OFF schedules (ECO_TEMPERATURE) to zones not currently needed
    for hub_name, all_zones in all_neohub_zones.items():
        for zone_name in all_zones:
            is_heating_needed = hub_name in zones_to_heat and zone_name in zones_to_heat[hub_name]
            
            if not is_heating_needed:
                await update_neohub_zone(hub_name, zone_name, ECO_TEMPERATURE)
                logging.info(f"Setting {zone_name} on {hub_name} to ECO temperature ({ECO_TEMPERATURE}°C) as no booking is active.")

    logging.info("Heating schedule update complete.")

def main():
    """Application entry point."""
    global config
    
    # Load configuration
    config = load_config(CONFIG_FILE)
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # Initialize NeoHub connections
    # Note: NEOHUB_* environment variables can override config.json here
    neohub_count = 1
    while True:
        name_env = os.environ.get(f"NEOHUB_{neohub_count}_NAME")
        if not name_env:
            break

        address = os.environ.get(f"NEOHUB_{neohub_count}_ADDRESS")
        port = os.environ.get(f"NEOHUB_{neohub_count}_PORT")
        token = os.environ.get(f"NEOHUB_{neohub_count}_TOKEN")
        
        if all([name_env, address, port, token]):
            port_int = int(port) if port.isdigit() else 4243
            config["neohubs"][name_env] = {
                "address": address,
                "port": port_int,
                "token": token,
            }
            logging.info(f"Loaded NeoHub config from ENV: {name_env}")

        neohub_count += 1
        
    for neohub_name, neohub_config in config["neohubs"].items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()
            
    # Optional: Log zones found on startup
    for neohub_name in config["neohubs"]:
        zones = asyncio.run(get_zones(neohub_name))
        if zones:
            logging.info(f"Zones on {neohub_name}: {zones}")
            if LOGGING_LEVEL == "DEBUG":
                logging.debug(f"main: Zones on {neohub_name}: {zones}")
                
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit()
        
    # Create a scheduler.
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    asyncio.run(update_heating_schedule())  # Run immediately
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()

    try:
        # This keeps the main thread alive, which is necessary for the scheduler to run.
        while True:
            time.sleep(600)
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler when the user presses Ctrl+C
        scheduler.shutdown()
        
if __name__ == "__main__":
    main()
