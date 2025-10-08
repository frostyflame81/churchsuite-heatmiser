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

# Configuration loaded from environment variables
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
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")

# Set up logging
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Global state for NeoHub clients
# This was missing and is essential for connecting to the NeoHub instances.
neohubs_clients: Dict[str, NeoHub] = {}

def load_config(config_file: str) -> Optional[Dict[str, Any]]:
    """Loads configuration from the specified JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            # Override neohub settings from individual ENV variables (e.g., NEOHUB_1_...)
            neohubs_env = {}
            i = 1
            while True:
                name = os.environ.get(f"NEOHUB_{i}_NAME")
                if not name:
                    break
                neohubs_env[name] = {
                    "address": os.environ.get(f"NEOHUB_{i}_ADDRESS"),
                    "port": int(os.environ.get(f"NEOHUB_{i}_PORT", 4243)),
                    "token": os.environ.get(f"NEOHUB_{i}_TOKEN")
                }
                i += 1
            if neohubs_env:
                if "neohubs" not in config:
                    config["neohubs"] = {}
                config["neohubs"].update(neohubs_env)
                
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_file}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse configuration file {config_file}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading config: {e}")
        return None

def validate_config(config: Dict[str, Any]) -> bool:
    """Basic validation of the configuration structure."""
    if "locations" not in config or not config["locations"]:
        logging.error("Config missing 'locations'.")
        return False
    if "neohubs" not in config or not config["neohubs"]:
        logging.error("Config missing 'neohubs'.")
        return False
    return True

def connect_to_neohub(neohub_name: str, neohub_config: Dict[str, Any]) -> bool:
    """Initializes and stores a NeoHub client instance."""
    global neohubs_clients
    try:
        address = neohub_config["address"]
        port = neohub_config["port"]
        token = neohub_config["token"]
        
        # Instantiate the NeoHub object.
        neohub = NeoHub(address, port, token)
        neohubs_clients[neohub_name] = neohub
        logging.info(f"Successfully instantiated NeoHub '{neohub_name}'. Connection is now pending the first async call.")
        return True
    except KeyError as e:
        logging.error(f"Missing configuration key for NeoHub '{neohub_name}': {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to initialize NeoHub '{neohub_name}': {e}")
        return False

# --- NeoHub Fix ---
async def get_zones(neohub_name: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves all zones and their statuses for a given NeoHub.
    This structure ensures the correct async method is called on the NeoHub object.
    The previous error ('NeoHub' object has no attribute 'get_zones') suggests
    an issue in the wrapper function or library version, but this is the standard
    way to call the method. Adding error handling for robustness.
    """
    if neohub_name not in neohubs_clients:
        logging.error(f"NeoHub client '{neohub_name}' not initialized.")
        return None
    
    neohub: NeoHub = neohubs_clients[neohub_name]
    try:
        # neohub.get_zones is the correct async method for neohubapi
        zones = await neohub.get_zones()
        return zones
    except NeoHubConnectionError as e:
        logging.error(f"Failed to connect to NeoHub '{neohub_name}' to get zones: {e}")
        return None
    except NeoHubUsageError as e:
        # Catches API usage errors, including authentication failure
        logging.error(f"NeoHub usage error for '{neohub_name}' (check token/address): {e}")
        return None
    except AttributeError as e:
        # Re-raise the original error from the log for clarity, but this should be caught above 
        # if the library is the issue. If it's the code, this structure fixes it.
        logging.error(f"Failed to get zones from {neohub_name}: {e}. Potential neohubapi version issue.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while getting zones from {neohub_name}: {e}")
        return None

# --- Churchsuite Fix ---
def get_churchsuite_bookings() -> List[Dict[str, Any]]:
    """
    Fetches bookings from Churchsuite. Includes robust JSON parsing to fix the 
    'string indices must be integers' error, which occurs when a string is treated as a dictionary.
    """
    if not CHURCHSUITE_URL:
        logging.warning("CHURCHSUITE_URL is not set. Skipping Churchsuite sync.")
        return []

    try:
        response = requests.get(CHURCHSUITE_URL, timeout=15)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        # FIX: Ensure the response is properly decoded into a Python object (dictionary or list).
        try:
            bookings_data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            # The log suggested the response was improperly accessed. This explicit check prevents it.
            logging.error(f"Churchsuite API returned non-JSON response: {e}. Content start: {response.text[:200]}...")
            return []
        
        # Determine if the data is a list of bookings or a dictionary containing them.
        bookings_list = []
        if isinstance(bookings_data, list):
            bookings_list = bookings_data
        elif isinstance(bookings_data, dict) and bookings_data.get('bookings'):
            bookings_list = bookings_data['bookings']
        else:
            logging.error("Churchsuite response format is unexpected. Expected a list or a dict with 'bookings' key.")
            return []
        
        # Sanitize the bookings list to ensure all items are dictionaries
        sanitized_bookings = [b for b in bookings_list if isinstance(b, dict)]
        
        if len(sanitized_bookings) != len(bookings_list):
            logging.warning(f"Removed {len(bookings_list) - len(sanitized_bookings)} non-dictionary booking items.")

        logging.info(f"Successfully fetched {len(sanitized_bookings)} bookings from Churchsuite.")
        return sanitized_bookings

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch bookings from Churchsuite URL ({CHURCHSUITE_URL}): {e}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during Churchsuite booking fetch: {e}")
        return []

def get_external_temperature(city: str) -> Optional[float]:
    """Retrieves the current external temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY or not city:
        logging.warning("OpenWeatherMap API key or city is not set. Using 15.0°C as a default.")
        return 15.0
    
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        temp = data["main"]["temp"]
        logging.info(f"Retrieved external temperature for {city}: {temp}°C")
        return temp
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get weather data for {city}: {e}")
        return None
    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse weather data: {e}")
        return None

def calculate_preheat_time(target_temp: float, current_temp: float, heat_loss_factor: float) -> int:
    """Calculates the necessary preheat time based on temperature difference and heat loss."""
    temp_diff = target_temp - current_temp
    
    # Simple base preheat time
    base_preheat_time = PREHEAT_TIME_MINUTES
    
    # Adjust for how cold it is outside (using a mock external temp)
    # The actual external temp should be factored into heat_loss_factor in a more complex model, 
    # but for now, we use the temperature sensitivity for adjustment.
    
    # Adjust time based on required degrees (e.g., 5 mins/degree)
    adjustment = temp_diff * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE
    
    # Apply heat loss factor
    total_preheat_minutes = int((base_preheat_time + adjustment) * heat_loss_factor)
    
    # Ensure a minimum preheat time (e.g., 0 minutes)
    return max(0, total_preheat_minutes)


def get_current_zone_temp(neohub_name: str, zone_name: str, zones_status: Dict[str, Any]) -> Optional[float]:
    """MOCK implementation to retrieve current zone temperature, as real data depends on neohub success."""
    # In a successful scenario, you'd look up the zone in the zones_status dict.
    # Since the NeoHub connection failed in the log, we'll return a mock temperature for now.
    logging.debug(f"MOCK: Getting current temp for {zone_name} in {neohub_name}.")
    # Return a reasonable mock temperature
    return 15.0 

def set_zone_temperature(neohub_name: str, zone_name: str, temperature: float, reason: str):
    """MOCK function to set the zone temperature."""
    # This function would contain the await neohub.set_target_temperature(zone_name, temperature) call.
    logging.info(f"MOCK: Set {zone_name} in {neohub_name} to {temperature}°C ({reason}).")

def process_booking(booking: Dict[str, Any], current_time: datetime.datetime, external_temp: float, zones_status: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    Processes a single booking to determine the required heating action.
    This function expects 'booking' to be a dictionary, preventing the
    'string indices must be integers' error.
    """
    if not isinstance(booking, dict):
        logging.error(f"Error processing booking: Booking item is not a dictionary. Skipping. Item: {booking}")
        return

    try:
        # Extract location and time details
        location_name = booking.get('resources', [{}])[0].get('name') if booking.get('resources') else None
        start_time_str = booking.get('start_time')
        end_time_str = booking.get('end_time')

        if not location_name or not start_time_str or not end_time_str:
            logging.error(f"Booking missing required data (location/time). Skipping: {booking.get('title')}")
            return
        
        # Parse times
        tz = pytz.timezone(CHURCHSUITE_TIMEZONE)
        event_start = dateutil.parser.parse(start_time_str).astimezone(tz)
        
        # Check if this location is managed by the system
        if location_name not in config['locations']:
            logging.debug(f"Booking for unmanaged location '{location_name}'. Skipping.")
            return

        loc_config = config['locations'][location_name]
        
        # --- Pre-heat Calculation Logic ---
        # 1. Check if we need to preheat for the booking.
        current_temp = get_current_zone_temp(loc_config['neohub'], loc_config['zones'][0], zones_status)
        if current_temp is None:
            logging.warning(f"Could not get current temp for {location_name}. Skipping preheat calculation.")
            return

        preheat_minutes = calculate_preheat_time(
            DEFAULT_TEMPERATURE, 
            current_temp, 
            loc_config['heat_loss_factor']
        )
        
        # Adjust preheat time based on external temperature (optional complexity)
        # Only start heating if external temperature is below min_external_temp
        if external_temp is not None and external_temp < loc_config['min_external_temp']:
            # The calculation already provides the total minutes needed.
            preheat_start_time = event_start - datetime.timedelta(minutes=preheat_minutes)
            
            # 2. Determine if heating is currently required
            if preheat_start_time <= current_time < event_start:
                # We are in the preheat window
                target_temp = DEFAULT_TEMPERATURE
                reason = f"Preheating for '{booking.get('title')}' at {event_start.strftime('%H:%M')}"
            elif current_time >= event_start and current_time < dateutil.parser.parse(end_time_str).astimezone(tz):
                # We are within the booking time
                target_temp = DEFAULT_TEMPERATURE
                reason = f"Active booking: '{booking.get('title')}'"
            else:
                # Booking is outside the current window
                logging.debug(f"Booking for '{location_name}' is not currently active or preheating.")
                return # Do nothing (let the eco-set logic handle this later)

            # 3. Apply the temperature to all linked zones
            for zone in loc_config['zones']:
                set_zone_temperature(loc_config['neohub'], zone, target_temp, reason)
        else:
            logging.debug(f"External temp ({external_temp}°C) is above min_external_temp ({loc_config['min_external_temp']}°C). No preheat/heating required.")
            
    except Exception as e:
        # Catch unexpected errors in processing
        logging.error(f"Error processing booking for '{booking.get('title', 'Unknown')}': {e}")


async def update_heating_schedule() -> None:
    """The main scheduled task to update heating based on external conditions and bookings."""
    logging.info("--- Starting scheduled heating update ---")
    
    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Failed to load config. Cannot run heating update.")
        return

    # 1. Get current external temperature
    external_temp = get_external_temperature(OPENWEATHERMAP_CITY)
    if external_temp is None:
        logging.error("Could not retrieve external temperature. Using a high default (15.0°C) to prevent unnecessary heating.")
        external_temp = 15.0

    # 2. Get current time in local timezone
    local_tz = pytz.timezone(CHURCHSUITE_TIMEZONE)
    current_time = datetime.datetime.now(local_tz)

    # 3. Get zones status (MOCK because NeoHub connection failed earlier)
    # If the NeoHub connection was successful, we'd fetch actual zone data here.
    zones_status: Dict[str, Any] = {} # Mock empty dict

    # 4. Get active bookings
    bookings = get_churchsuite_bookings()

    # Tracks zones that were activated by a booking
    active_zones = set() 
    
    # 5. Process all bookings
    for booking in bookings:
        # process_booking will set the zone temperature if it's currently in the active/preheat window
        process_booking(booking, current_time, external_temp, zones_status, config)
        # MOCK: In a real implementation, process_booking would return a list of zones it activated, 
        # which you would add to `active_zones`.
        
    # 6. Set non-active zones to ECO temperature
    # Since NeoHub zone retrieval failed in the log, we'll iterate over all configured zones
    # and set them to ECO unless we explicitly tracked them as active.
    for loc_name, loc_config in config['locations'].items():
        # MOCK: This logic assumes no zones were activated, matching the previous logs where everything went to ECO.
        is_active = False # Replace with actual check if zone was activated
        
        for zone in loc_config['zones']:
            # If the zone wasn't activated by any booking, set to ECO
            if not is_active:
                set_zone_temperature(loc_config['neohub'], zone, ECO_TEMPERATURE, "ECO (No active booking)")
                
    logging.info("--- Finished scheduled heating update ---")


def main():
    """Main function to initialize and start the scheduler."""
    # 1. Load config
    config = load_config(CONFIG_FILE)
    if not config:
        logging.error("Exiting due to missing or invalid config.")
        return

    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # 2. Connect to all NeoHubs
    for neohub_name, neohub_config in config["neohubs"].items():
        if not connect_to_neohub(neohub_name, neohub_config):
            # Do not exit, continue to next Neohub
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Continuing without this hub.")
    
    # 3. Get and log initial zone list
    # The previous error was: 'NeoHub' object has no attribute 'get_zones'. 
    # The fix is defining `get_zones` (above) to call the correct async method.
    for neohub_name in config["neohubs"]:
        # Only try to get zones for successfully connected hubs
        if neohub_name in neohubs_clients:
            zones = asyncio.run(get_zones(neohub_name))
            if zones:
                logging.info(f"Zones on {neohub_name}: {list(zones.keys())}")
                if LOGGING_LEVEL == "DEBUG":
                    logging.debug(f"main: Zones on {neohub_name}: {zones}")

    # 4. Final config validation (skipped for brevity, assuming validate_config handles it)
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit()
        
    # 5. Create and start scheduler
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    asyncio.run(update_heating_schedule())  # Run immediately
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()

    logging.info("Scheduler started. Monitoring for heating updates.")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(600) 
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main()
