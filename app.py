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
# Import necessary classes and exceptions from neohubapi
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError, WebSocketClient # type: ignore

# --- Configuration & Global State ---

# Environment variables
# Note: Using float() for temperatures ensures compatibility with decimal values in ENV
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE")
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
    level=LOGGING_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Global state to store connected NeoHub instances
NEOHUBS: Dict[str, NeoHub] = {}
# Global variable to store loaded configuration
config: Dict[str, Any] = {}

# --- Utility Functions ---

def load_config(filepath: str) -> Optional[Dict[str, Any]]:
    """Loads the configuration from a JSON file and environment variables."""
    try:
        with open(filepath, "r") as f:
            base_config = json.load(f)
        
        # Load NeoHub config from ENV variables if present (NEOHUB_1_NAME, etc.)
        neohubs_from_env = {}
        i = 1
        while True:
            name = os.environ.get(f"NEOHUB_{i}_NAME")
            if not name:
                break
            neohubs_from_env[name] = {
                "address": os.environ.get(f"NEOHUB_{i}_ADDRESS"),
                "port": int(os.environ.get(f"NEOHUB_{i}_PORT", 4243)),
                "token": os.environ.get(f"NEOHUB_{i}_TOKEN"),
            }
            logging.info(f"Loaded NeoHub config from ENV: {name}")
            i += 1
        
        if neohubs_from_env:
            # Merge/overwrite 'neohubs' from environment variables
            if "neohubs" in base_config:
                base_config["neohubs"].update(neohubs_from_env)
            else:
                base_config["neohubs"] = neohubs_from_env
            
        return base_config

    except FileNotFoundError:
        logging.error(f"Configuration file not found at {filepath}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from {filepath}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading config: {e}")
        return None

def validate_config(config: Dict[str, Any]) -> bool:
    """Validates the structure of the loaded configuration."""
    if "locations" not in config or "neohubs" not in config:
        logging.error("Config missing 'locations' or 'neohubs' sections.")
        return False
    return True

# --- NeoHub Connection Logic (INITIALIZATION ROUTINE FIX) ---

def connect_to_neohub(neohub_name: str, neohub_config: Dict[str, Any]) -> bool:
    """
    Instantiates and stores a NeoHub object for later use.
    
    REPAIR: Removed the explicit 'await hub.connect()' call which caused the
    AttributeError in newer versions of neohubapi. The connection is now 
    handled implicitly upon object instantiation (NeoHub(...)).
    """
    logging.info(
        f"Attempting connection to NeoHub '{neohub_name}' at {neohub_config['address']}:{neohub_config['port']}"
    )
    try:
        # Instantiate the NeoHub object. This is the crucial step.
        neohub = NeoHub(
            neohub_config["address"],
            neohub_config["port"],
            neohub_config["token"],
            config=neohub_config,
        )
        NEOHUBS[neohub_name] = neohub
        logging.info(
            f"Successfully instantiated NeoHub '{neohub_name}'. Connection is now pending the first async call."
        )
        return True
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during connection attempt for NeoHub {neohub_name}: {e}"
        )
        return False

# --- Core Asynchronous Functions ---

async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Gets the list of zones from a specified NeoHub."""
    if neohub_name not in NEOHUBS:
        logging.warning(f"NeoHub '{neohub_name}' not connected.")
        return None
    
    neohub = NEOHUBS[neohub_name]
    try:
        # The first call to an async method (like get_zones) will establish the connection
        zones_data = await neohub.get_zones()
        # Return a list of zone names
        return [z.get('name', 'Unknown Zone') for z in zones_data if isinstance(z, dict)]
    except NeoHubConnectionError as e:
        logging.error(f"Connection failed for {neohub_name} during get_zones: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to get zones from {neohub_name}: {e}")
        return None

async def get_weather_data() -> Optional[float]:
    """Fetches external temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY or not OPENWEATHERMAP_CITY:
        logging.error("OpenWeatherMap configuration is missing. Cannot fetch external temp.")
        return None
        
    url = (
        f"http://api.openweathermap.org/data/2.5/weather?q={OPENWEATHERMAP_CITY}"
        f"&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        external_temp = data["main"]["temp"]
        logging.info(f"Retrieved external temperature for {OPENWEATHERMAP_CITY}: {external_temp}°C")
        return external_temp
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None
    except (KeyError, TypeError) as e:
        logging.error(f"Unexpected structure in weather data response: {e}")
        return None


async def get_churchsuite_bookings() -> List[Dict[str, Any]]:
    """Fetches upcoming Churchsuite bookings."""
    if not CHURCHSUITE_URL:
        logging.warning("Churchsuite URL is not configured.")
        return []
    
    # We fetch all bookings today and tomorrow to cover pre-heating needs (simplified here)
    try:
        # Assuming the URL returns a JSON list of bookings directly
        response = requests.get(CHURCHSUITE_URL, timeout=15)
        response.raise_for_status()
        bookings_data = response.json()
        
        logging.info(f"Successfully fetched {len(bookings_data)} bookings from Churchsuite.")
        return bookings_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Churchsuite bookings: {e}")
        return []

def calculate_preheat_minutes(target_temp: float, current_temp: float, external_temp: float, heat_loss_factor: float) -> int:
    """
    Calculates the required pre-heat time based on temperature delta and heat loss.
    
    Args:
        target_temp (float): The desired setpoint temperature.
        current_temp (float): The current measured temperature.
        external_temp (float): The current outdoor temperature.
        heat_loss_factor (float): Location-specific factor indicating how quickly heat is lost.
    
    Returns:
        int: The number of minutes required for pre-heating.
    """
    temp_delta = target_temp - current_temp
    
    if temp_delta <= 0:
        return 0 # Already at or above target temperature
        
    # Adjustment for external temperature (lower external temp increases preheat time)
    # The 'min_external_temp' (from config/location) acts as a baseline.
    min_external_temp = config.get('min_external_temp', 5.0) # Using float fallback
    external_impact = max(0.0, min_external_temp - external_temp) * 0.5 # Simple heuristic (0.5 mins per degree below baseline)
    
    # Calculate base preheat time
    base_preheat = PREHEAT_TIME_MINUTES 
    
    # Adjust based on temperature delta (how many degrees to rise)
    preheat_adjustment = temp_delta * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE
    
    # Apply heat loss factor
    total_preheat_minutes = int((base_preheat + preheat_adjustment + external_impact) * heat_loss_factor)
    
    # Ensure preheat time is reasonable (e.g., max 4 hours)
    return min(total_preheat_minutes, 240)


async def update_heating_schedule():
    """Main job that runs on a schedule to adjust heating."""
    logging.info("--- Starting scheduled heating update ---")

    current_external_temp = await get_weather_data()
    if current_external_temp is None:
        logging.error("Could not retrieve external temperature. Skipping update.")
        return

    bookings = await get_churchsuite_bookings()

    tz_str = os.environ.get("CHURCHSUITE_TIMEZONE", "UTC")
    try:
        tz = pytz.timezone(tz_str)
    except pytz.exceptions.UnknownTimeZoneError:
        logging.error(f"Unknown timezone: {tz_str}. Defaulting to UTC.")
        tz = pytz.utc
        
    now = datetime.datetime.now(tz)
    
    # Track zones that have a valid booking to ensure we don't turn them off
    active_zones: Dict[str, datetime.datetime] = {} # Key: NeohubName:ZoneName, Value: Target End Time

    # Process bookings
    for booking in bookings:
        try:
            # Assume booking has 'start_time' and 'end_time' fields that are parsable datetime strings
            booking_start = dateutil.parser.parse(booking['start_time']).astimezone(tz)
            booking_end = dateutil.parser.parse(booking['end_time']).astimezone(tz)
            
            # Match booking location to configured zone
            location_name = booking.get('location_name') 
            if location_name and location_name in config.get("locations", {}):
                location_config = config["locations"][location_name]
                neohub_name = location_config["neohub"]
                target_zones = location_config["zones"]
                
                target_temp = location_config.get("default_temperature", DEFAULT_TEMPERATURE) 
                
                # Only process bookings starting in the next 4 hours
                if booking_start > now and booking_start < now + datetime.timedelta(hours=4):
                    
                    # NOTE: To implement properly, you would need to iterate through target_zones
                    # and get the current temp for each, but we'll stick to a mock for now
                    # current_temp = await NEOHUBS[neohub_name].get_live_temp(target_zones[0])
                    current_temp = 15.0 # MOCK temperature for logic testing
                    
                    preheat_minutes = calculate_preheat_minutes(
                        target_temp, 
                        current_temp, 
                        current_external_temp, 
                        location_config["heat_loss_factor"]
                    )
                    
                    preheat_start_time = booking_start - datetime.timedelta(minutes=preheat_minutes)
                    
                    if now >= preheat_start_time:
                        logging.info(
                            f"Booking for {location_name} starts at {booking_start.strftime('%H:%M')}. "
                            f"Setting zones {target_zones} in '{neohub_name}' to {target_temp}°C (Preheat {preheat_minutes}m)."
                        )
                        # The actual setpoint call
                        for zone in target_zones:
                            # await NEOHUBS[neohub_name].set_setpoint(zone, target_temp) # Actual command
                            active_zones[f"{neohub_name}:{zone}"] = booking_end
                            logging.debug(f"MOCK: Set {zone} in {neohub_name} to {target_temp}°C.")
                            
        except Exception as e:
            logging.error(f"Error processing booking: {booking}. Error: {e}")
            continue

    # Logic to ensure all non-active/past-active zones are set to ECO temperature
    for location_name, location_config in config.get("locations", {}).items():
        neohub_name = location_config["neohub"]
        for zone in location_config["zones"]:
            zone_key = f"{neohub_name}:{zone}"
            
            # Check if zone is currently active/preheating
            if zone_key in active_zones:
                # If the booking end time has passed, set back to ECO
                if now > active_zones[zone_key]:
                    logging.info(f"Booking ended for {zone} in {neohub_name}. Setting to ECO.")
                    # await NEOHUBS[neohub_name].set_setpoint(zone, ECO_TEMPERATURE) # Actual command
                    logging.debug(f"MOCK: Set {zone} in {neohub_name} to {ECO_TEMPERATURE}°C (After Event).")
                # If active, leave alone.
                
            else:
                # Zone has no booking at all, set to default ECO
                logging.debug(f"No active booking for {zone} in {neohub_name}. Setting to ECO.")
                # await NEOHUBS[neohub_name].set_setpoint(zone, ECO_TEMPERATURE) # Actual command
                logging.debug(f"MOCK: Set {zone} in {neohub_name} to {ECO_TEMPERATURE}°C (Default).")

    logging.info("--- Finished scheduled heating update ---")


def main():
    """Application entry point."""
    global config # Make config available to other functions

    # Set up logging before doing anything else
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    temp_config = load_config(CONFIG_FILE)
    if temp_config is None:
        logging.error("Could not load configuration. Exiting.")
        return
    
    global config
    config = temp_config
    
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # Connect/Instantiate all NeoHubs using the repaired routine
    for neohub_name, neohub_config in config.get("neohubs", {}).items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            # Use os._exit(1) or raise for non-scheduler driven applications
            exit(1) 
    
    # Log zones for all hubs (this is the first async operation that forces the connection handshake)
    for neohub_name in config.get("neohubs", {}):
        # get_zones is an async function, so we must run it in an event loop
        zones = asyncio.run(get_zones(neohub_name))
        if zones:
            logging.info(f"Zones on {neohub_name}: {zones}")
            if LOGGING_LEVEL == "DEBUG":
                logging.debug(f"main: Zones on {neohub_name}: {zones}")

    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit(1)

    # Create a scheduler.
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    # Note: Using asyncio.run inside a non-async function is valid for job scheduling.
    asyncio.run(update_heating_schedule())  # Run immediately
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()

    logging.info("Scheduler started. Monitoring for heating updates.")
    try:
        # Keep the main thread alive for the scheduler
        while True:
            time.sleep(600) 
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down scheduler.")
        scheduler.shutdown()

if __name__ == "__main__":
    main()
