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

# --- Global Configuration from Environment Variables ---
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
# Ensure a timezone is set for local time calculations (default to UTC if not provided)
CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "UTC") 
PREHEAT_TIME_MINUTES = int(os.environ.get("PREHEAT_TIME_MINUTES", 30))
DEFAULT_TEMPERATURE = float(os.environ.get("DEFAULT_TEMPERATURE", 19))
ECO_TEMPERATURE = float(os.environ.get("ECO_TEMPERATURE", 12))
TEMPERATURE_SENSITIVITY = int(os.environ.get("TEMPERATURE_SENSITIVITY", 10))
PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE = float(
    os.environ.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 5)
)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()

# --- NeoHub Globals and Connection Logic ---
# Global store for connected NeoHub instances
NEOHUBS: Dict[str, NeoHub] = {}

def connect_to_neohub(name: str, config: Dict[str, Any]) -> bool:
    """
    Initializes and stores a NeoHub connection.
    
    FIX: This function now explicitly passes the 'token' to the NeoHub constructor,
    solving the 'You must provide a token for a connection on port 4243' error.
    """
    address = config.get("address")
    port = config.get("port", 4243)
    token = config.get("token")
    
    if not address:
        logging.error(f"NeoHub '{name}' is missing an 'address' in configuration.")
        return False
        
    try:
        # Pass the token explicitly from the config. NeoHub assumes a secure connection 
        # on port 4243, which requires the token.
        hub = NeoHub(
            ip_address=address, 
            token=token, 
            port=port,
            secure=True if port == 4243 else False,
            timeout=10
        )
        NEOHUBS[name] = hub
        logging.info(f"Successfully initialized NeoHub '{name}' at {address}:{port}.")
        return True
    except (NeoHubConnectionError, NeoHubUsageError) as e:
        # Catch specific NeoHub errors, including the missing token error.
        logging.error(f"Failed to initialize NeoHub '{name}': {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while connecting to NeoHub '{name}': {e}")
        return False

async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Retrieves the list of zones from a NeoHub."""
    hub = NEOHUBS.get(neohub_name)
    if not hub:
        logging.warning(f"NeoHub '{neohub_name}' is not connected.")
        return None
    try:
        zones = await hub.get_zones()
        return zones
    except Exception as e:
        logging.error(f"Error getting zones from {neohub_name}: {e}")
        return None
        
# --- Configuration Loading and Validation ---

def load_config(config_file: str) -> Optional[Dict[str, Any]]:
    """Loads configuration from a JSON file."""
    config = {}
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
            
        # Ensure neohubs section is present (it is in the log config)
        if "neohubs" not in config:
            config["neohubs"] = {}
            
        return config
        
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_file}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON in {config_file}: {e}")
        return None
    except Exception as e:
        logging.error(f"An error occurred while loading configuration: {e}")
        return None

def validate_config(config: Dict[str, Any]) -> bool:
    """Performs basic validation on the loaded configuration."""
    if not config.get("neohubs"):
        logging.error("No NeoHubs configured.")
        return False
    if not config.get("locations"):
        logging.error("No locations configured.")
        return False
    return True

# --- External Data Retrieval Functions ---

async def get_weather_data() -> Optional[float]:
    """Retrieves external temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY or not OPENWEATHERMAP_CITY:
        logging.warning("OpenWeatherMap API key or City is not set. Cannot fetch external temperature.")
        return None

    url = (
        f"http://api.openweathermap.org/data/2.5/weather?q={OPENWEATHERMAP_CITY}&"
        f"appid={OPENWEATHERMAP_API_KEY}&units=metric"
    )
    try:
        # Use asyncio.to_thread for synchronous requests call
        response = await asyncio.to_thread(requests.get, url, timeout=10)
        response.raise_for_status()
        data = response.json()
        temp = data["main"]["temp"]
        logging.info(f"Retrieved external temperature for {OPENWEATHERMAP_CITY}: {temp}°C")
        return temp
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None
    except Exception as e:
        logging.error(f"Error parsing weather data: {e}")
        return None


async def get_churchsuite_bookings() -> List[Dict[str, Any]]:
    """
    Retrieves and parses bookings from ChurchSuite.
    
    FIX: The parsing logic is now more robust. It checks the type of the received 
    data and logs a more detailed error if the structure is not as expected.
    """
    if not CHURCHSUITE_URL:
        logging.warning("ChurchSuite URL is not set. Cannot fetch bookings.")
        return []

    try:
        # Use asyncio.to_thread for synchronous requests call
        response = await asyncio.to_thread(requests.get, CHURCHSUITE_URL, timeout=15)
        response.raise_for_status() # Check for bad HTTP status codes

        data = response.json()
        
        bookings: List[Dict[str, Any]] = []

        if isinstance(data, list):
            # Case 1: The response is a top-level list of bookings
            bookings = data
        elif isinstance(data, dict) and 'bookings' in data and isinstance(data['bookings'], list):
            # Case 2: The response is a dictionary containing a 'bookings' key which is a list
            bookings = data['bookings']
        else:
            # FIX: Log a more informative error about the unexpected data structure
            keys_info = list(data.keys()) if isinstance(data, dict) else 'N/A'
            logging.error(
                f"Churchsuite response format is unexpected. Expected a list or a dict with 'bookings' key. "
                f"Received type: {type(data).__name__}. Keys (if dict): {keys_info}"
            )
            return []

        logging.info(f"Successfully retrieved {len(bookings)} bookings from ChurchSuite.")
        return bookings

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching ChurchSuite data: {e}")
        return []
    except json.JSONDecodeError as e:
        # Log the beginning of the response text if JSON parsing fails
        response_text = getattr(response, 'text', '')
        logging.error(f"Error decoding ChurchSuite JSON response: {e}. Response was: {response_text[:100]}...")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during ChurchSuite fetch: {e}")
        return []

# --- Heating Logic Functions ---

def calculate_preheat_time(
    current_temp: float, 
    target_temp: float, 
    heat_loss_factor: float, 
    preheat_adjustment_minutes_per_degree: float, 
    default_preheat_time_minutes: int
) -> datetime.timedelta:
    """Calculates the necessary preheat time based on temperature delta and heat loss."""
    temp_delta = target_temp - current_temp
    
    # Only preheat if the target is higher than the current temp
    if temp_delta <= 0:
        return datetime.timedelta(minutes=0)

    # Start with a minimum preheat time
    adjusted_minutes = float(default_preheat_time_minutes)

    # Calculate additional time based on temperature difference and heat loss factor
    time_adjustment = (
        temp_delta * preheat_adjustment_minutes_per_degree * heat_loss_factor
    )
    
    adjusted_minutes += time_adjustment

    # Ensure preheat time is non-negative
    preheat_minutes = max(0.0, adjusted_minutes)

    return datetime.timedelta(minutes=preheat_minutes)


async def set_zone_temperature(
    neohub_name: str, 
    zone_name: str, 
    temperature: float, 
    reason: str
):
    """Sets the temperature on the NeoHub or logs a MOCK if connection failed."""
    hub = NEOHUBS.get(neohub_name)
    if not hub:
        # MOCK: This path is taken when NeoHub initialization fails.
        logging.info(f"MOCK: Set {zone_name} in {neohub_name} to {temperature}°C ({reason}).")
        return
        
    try:
        # Hold the temperature for a duration longer than the scheduler interval (e.g., 2 hours)
        await hub.set_zone_temp(zone_name, temperature, 120)
        logging.info(f"ACTUAL: Set {zone_name} in {neohub_name} to {temperature}°C ({reason}).")
    except Exception as e:
        logging.error(f"Failed to set temperature for {zone_name} on {neohub_name}: {e}")


async def update_heating_schedule():
    """The main logic function to check bookings and adjust heating."""
    logging.info("--- Starting scheduled heating update ---")

    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Config not loaded for heating update.")
        return
        
    # 1. Get current external temperature
    external_temp = await get_weather_data()
    
    # 2. Get bookings
    all_bookings = await get_churchsuite_bookings()
    
    # Set the local timezone for comparison
    try:
        local_tz = pytz.timezone(CHURCHSUITE_TIMEZONE)
    except Exception:
        logging.error(f"Invalid timezone setting: {CHURCHSUITE_TIMEZONE}. Defaulting to UTC.")
        local_tz = pytz.utc
        
    now = datetime.datetime.now(local_tz)

    # Dictionary to track the required temperature and preheat time for each physical NeoHub zone
    # Initialized with ECO settings for all configured NeoHub zones
    zone_heating_schedule: Dict[str, Dict[str, Dict[str, Any]]] = {}
    
    for location_name, location_config in config["locations"].items():
        hub_name = location_config["neohub"]
        if hub_name not in zone_heating_schedule:
            zone_heating_schedule[hub_name] = {}
        
        for zone_name in location_config["zones"]:
            if zone_name not in zone_heating_schedule[hub_name]:
                zone_heating_schedule[hub_name][zone_name] = {
                    'target_temp': ECO_TEMPERATURE,
                    'preheat_start': now + datetime.timedelta(days=365), # Far future
                    'reason': 'ECO (No active booking)',
                }

    # 3. Process Bookings to find the max required temperature and earliest preheat time
    for booking in all_bookings:
        try:
            start_str = booking.get('start')
            end_str = booking.get('end')
            location_name = booking.get('location')
            summary = booking.get('summary', 'Unknown Event')

            if not start_str or not end_str or not location_name:
                logging.warning(f"Skipping malformed booking: {booking}")
                continue
                
            # Parse times and localize
            start_time = dateutil.parser.parse(start_str).astimezone(local_tz)
            # end_time = dateutil.parser.parse(end_str).astimezone(local_tz) # not used directly for schedule
            
            # Check if the booking start time has passed and not for a future date
            if start_time.date() < now.date() and start_time.date() != now.date():
                continue # Skip past bookings

            location_config = config["locations"].get(location_name)
            if not location_config:
                if LOGGING_LEVEL == "DEBUG":
                    logging.debug(f"Booking location '{location_name}' not in config. Skipping.")
                continue

            # Determine Target Temperature
            min_external_temp = location_config["min_external_temp"]
            target_temp = DEFAULT_TEMPERATURE
            
            # Check if external temperature is above the minimum required for heating
            if external_temp is not None and external_temp > min_external_temp:
                target_temp = ECO_TEMPERATURE
            
            # If target_temp is ECO, no heating is needed for this location
            if target_temp <= ECO_TEMPERATURE:
                continue
                
            # Calculate Preheat Time
            heat_loss_factor = location_config["heat_loss_factor"]
            
            # MOCK current temp for calculation (assuming ECO temp if no real-time data)
            mock_current_temp = ECO_TEMPERATURE 

            preheat_duration = calculate_preheat_time(
                current_temp=mock_current_temp,
                target_temp=target_temp,
                heat_loss_factor=heat_loss_factor,
                preheat_adjustment_minutes_per_degree=PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE,
                default_preheat_time_minutes=PREHEAT_TIME_MINUTES
            )
            preheat_start = start_time - preheat_duration
            
            # Check if preheat has already started or is due to start soon (i.e., preheat start is now or in the future)
            # We are controlling the heating if the preheat has started, or if the booking is currently active.
            is_active_now = now >= preheat_start

            # If the booking requires heating and is active/imminent, update the zone schedule
            if is_active_now:
                hub_name = location_config["neohub"]
                reason = f"BOOKING (Event: {summary} @ {start_time.strftime('%H:%M')})"

                for zone_name in location_config["zones"]:
                    # Check if hub exists in the schedule before accessing it
                    if hub_name not in zone_heating_schedule or zone_name not in zone_heating_schedule[hub_name]:
                        logging.warning(f"Zone {zone_name} or hub {hub_name} missing from schedule initialization. Skipping.")
                        continue
                        
                    current_zone_schedule = zone_heating_schedule[hub_name][zone_name]
                    
                    # Take the highest temperature requested and the earliest preheat start time
                    if target_temp > current_zone_schedule['target_temp']:
                        current_zone_schedule['target_temp'] = target_temp
                        current_zone_schedule['preheat_start'] = preheat_start
                        current_zone_schedule['reason'] = reason
                    # If temps are equal, keep the earliest preheat start time
                    elif target_temp == current_zone_schedule['target_temp'] and preheat_start < current_zone_schedule['preheat_start']:
                        current_zone_schedule['preheat_start'] = preheat_start
                        current_zone_schedule['reason'] = reason


        except Exception as e:
            logging.error(f"Error processing booking {booking}: {e}")
            continue

    # 4. Apply the final schedule to the physical NeoHub zones
    for hub_name, zones_schedule in zone_heating_schedule.items():
        for zone_name, schedule in zones_schedule.items():
            final_temp = schedule['target_temp']
            reason = schedule['reason']
            await set_zone_temperature(hub_name, zone_name, final_temp, reason)

    logging.info("--- Finished scheduled heating update ---")


def main():
    """Main application loop and scheduler setup."""
    logging.basicConfig(
        level=getattr(logging, LOGGING_LEVEL, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting Churchsuite-Heatmiser scheduler...")
    
    # 1. Load config
    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Configuration failed to load. Exiting.")
        return
        
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # 2. Connect to all NeoHubs
    connected_hubs = 0
    for neohub_name, neohub_config in config["neohubs"].items():
        if connect_to_neohub(neohub_name, neohub_config):
            connected_hubs += 1
        else:
            # FIX: Log the failure and continue to the next hub (matching the log output)
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Continuing without this hub.")
    
    # If no hubs connected, log a warning and continue to run in MOCK mode (as seen in the original log)
    if connected_hubs == 0 and config["neohubs"]:
        logging.warning("Failed to connect to ANY configured NeoHubs. The application will run in MOCK mode.")

    # 3. Validate config
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        return

    # 4. Create and start scheduler
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    asyncio.run(update_heating_schedule())  # Run immediately
    
    # Schedule the job
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()

    logging.info("Scheduler started. Monitoring for heating updates.")

    try:
        # Keep the main thread alive.
        while True:
            time.sleep(600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Scheduler shut down.")

if __name__ == "__main__":
    main()
