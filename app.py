import asyncio
import datetime
import json
import logging
import time
import requests # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler # type: ignore
import os
import pytz # type: ignore
import dateutil.parser # type: ignore
from typing import Dict, Any, List, Optional
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError # type: ignore

# --- Configuration & Globals ---

# Configuration loaded from Environment Variables
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
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
logging.basicConfig(level=getattr(logging, LOGGING_LEVEL),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Global state for NeoHub connections and configs
NEOHUBS: Dict[str, Optional[NeoHub]] = {}
NEOHUB_CONFIGS: Dict[str, Dict[str, Any]] = {} 
config: Dict[str, Any] = {}
try:
    LOCAL_TZ = pytz.timezone(TIMEZONE)
except pytz.UnknownTimeZoneError:
    logging.warning(f"Unknown timezone: {TIMEZONE}. Defaulting to UTC.")
    LOCAL_TZ = pytz.utc


# --- NeoHub Configuration Parsing ---

def _load_neohub_config_from_env() -> Dict[str, Dict[str, Any]]:
    """
    Parses NEOHUB_X_* environment variables to create a configuration dictionary.
    """
    neohub_configs: Dict[str, Dict[str, Any]] = {}
    i = 1
    while True:
        name_key = f"NEOHUB_{i}_NAME"
        address_key = f"NEOHUB_{i}_ADDRESS"
        port_key = f"NEOHUB_{i}_PORT"
        token_key = f"NEOHUB_{i}_TOKEN"

        name = os.environ.get(name_key)
        
        if not name:
            break 

        address = os.environ.get(address_key)
        port_str = os.environ.get(port_key)
        token = os.environ.get(token_key)
        port = int(port_str) if port_str and port_str.isdigit() else 4242

        if not address:
            logging.warning(f"NeoHub '{name}' defined but missing {address_key}. Skipping.")
            i += 1
            continue
        
        if port == 4243 and not token:
            logging.error(f"NeoHub '{name}' is configured for secure port 4243 but is missing a token. Check {token_key}.")
            i += 1
            continue

        neohub_configs[name] = {
            "address": address,
            "port": port,
            "token": token,
        }
        logging.info(f"Loaded NeoHub config from ENV: {name}")

        i += 1
    
    return neohub_configs

# --- Utility Functions ---

def load_config() -> Optional[Dict[str, Any]]:
    """Loads configuration from file and environment variables."""
    global NEOHUB_CONFIGS
    
    local_config: Dict[str, Any] = {}
    try:
        # Load from JSON file first
        with open(CONFIG_FILE, "r") as f:
            local_config = json.load(f)
        logging.info(f"Loaded configuration from {CONFIG_FILE}")
    except FileNotFoundError:
        logging.error(f"Config file not found: {CONFIG_FILE}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in config file: {CONFIG_FILE}")
        return None
    
    # Load and merge environment NeoHub configs into the main config structure
    NEOHUB_CONFIGS = _load_neohub_config_from_env()
    
    # Initialize 'neohubs' key if missing (Fix for KeyError)
    if "neohubs" not in local_config:
        local_config["neohubs"] = {}

    # Environment variables override/add to the file config
    local_config["neohubs"].update(NEOHUB_CONFIGS)
    
    # Update global constants based on config file (if not set by ENV in python code)
    # Note: These values were read from ENV at the top of the file, but we should 
    # use the values from the config file if they exist and haven't been loaded via ENV.
    local_config["preheat_time_minutes"] = local_config.get("preheat_time_minutes", PREHEAT_TIME_MINUTES)
    local_config["default_temperature"] = local_config.get("default_temperature", DEFAULT_TEMPERATURE)
    local_config["eco_temperature"] = local_config.get("eco_temperature", ECO_TEMPERATURE)
    local_config["temperature_sensitivity"] = local_config.get("temperature_sensitivity", TEMPERATURE_SENSITIVITY)
    local_config["preheat_adjustment_minutes_per_degree"] = local_config.get("preheat_adjustment_minutes_per_degree", PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE)
    
    return local_config

def get_external_temperature(city: str, api_key: str) -> Optional[float]:
    """Fetches the current external temperature using OpenWeatherMap."""
    if not api_key or not city:
        logging.warning("OpenWeatherMap API key or city is missing. Cannot fetch external temperature.")
        return None
        
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
    """
    
    base_preheat = config["preheat_time_minutes"]
    adjustment_factor = config["preheat_adjustment_minutes_per_degree"]
    
    # Temperature Differential (How much heat is lost to the outside)
    temp_differential = max(0.0, target_temp - external_temp)
    
    # Adjustment based on differential and heat loss
    adjustment = (
        heat_loss_factor * temp_differential * adjustment_factor
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

def validate_config(cfg: Dict[str, Any]) -> bool:
    """Validates configuration keys and checks for necessary external settings."""
    is_valid = True
    
    if not cfg.get("neohubs"):
        logging.error("Config validation failed: 'neohubs' is empty. No hubs configured.")
        is_valid = False
        
    if not cfg.get("locations"):
        logging.error("Config validation failed: 'locations' key is missing or empty.")
        is_valid = False
    
    for loc_name, loc_config in cfg.get("locations", {}).items():
        neohub_name = loc_config.get("neohub")
        if not neohub_name or neohub_name not in cfg["neohubs"]:
            logging.error(f"Config validation failed: Location '{loc_name}' refers to an unknown 'neohub': {neohub_name}.")
            is_valid = False
        if not loc_config.get("zones") or not isinstance(loc_config["zones"], list):
            logging.error(f"Config validation failed: Location '{loc_name}' is missing or has invalid 'zones'.")
            is_valid = False

    if not CHURCHSUITE_URL:
        logging.error("Config validation failed: CHURCHSUITE_URL environment variable is missing.")
        is_valid = False
    
    if not OPENWEATHERMAP_API_KEY or not OPENWEATHERMAP_CITY:
        logging.warning("OpenWeatherMap details are missing. Heating calculations will rely only on base preheat time if min_external_temp is hit.")

    return is_valid

# --- NeoHub Interaction Functions (Async) ---

async def connect_to_neohub(name: str, neohub_config: Dict[str, Any]) -> bool:
    """Connects to a single NeoHub and stores the connection object."""
    try:
        address = neohub_config.get("address")
        port = neohub_config.get("port", 4242)
        token = neohub_config.get("token")
        
        if port == 4243 and not token:
            # Raise the specific error the library checks for
            raise NeoHubConnectionError(
                "You must provide a token for a connection on port 4243, or use a legacy connection on port 4242"
            )

        logging.info(f"Attempting connection to NeoHub '{name}' at {address}:{port}")

        hub = NeoHub(
            address,
            port,
            token=token,
            loop=asyncio.get_event_loop()
        )
        await hub.connect()
        NEOHUBS[name] = hub
        logging.info(f"Successfully connected to NeoHub: {name}")
        return True
    except NeoHubConnectionError as e:
        logging.error(f"Connection error initializing NeoHub {name}: {e}")
        NEOHUBS[name] = None
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during connection to NeoHub {name}: {e}")
        NEOHUBS[name] = None
        return False

async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Retrieves zones from a specific NeoHub."""
    neohub = NEOHUBS.get(neohub_name)
    if not neohub:
        logging.warning(f"NeoHub '{neohub_name}' is not connected or initialized.")
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
        
        sensitivity_offset = config["temperature_sensitivity"] / 100.0 # e.g. 10/100 = 0.1

        if current_zone_info:
            current_temp = current_zone_info.get('temp')
            if current_temp is not None and current_temp >= temp - sensitivity_offset:
                 logging.debug(
                    f"Zone {zone_name} is already at {current_temp}°C. "
                    f"Skipping temperature set as it meets the target {temp}°C (offset: {sensitivity_offset}°C)."
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

# --- Core Scheduling Logic (Async) ---

async def update_heating_schedule():
    """Main function to fetch bookings, calculate schedules, and update NeoHubs."""
    logging.info("Starting heating schedule update...")
    
    # 1. Fetch external temperature
    external_temp = get_external_temperature(
        OPENWEATHERMAP_CITY, OPENWEATHERMAP_API_KEY
    )
    
    if external_temp is None:
        # Use min_external_temp of 0 as a safe fallback if weather API fails
        logging.warning("Using 0°C as fallback external temperature for calculation.")
        external_temp = 0.0

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
    
    # Use 'booked_resources' and 'resources' based on the confirmed API structure
    bookings = data.get("booked_resources", []) 
    resource_map = data.get("resources", {})

    if not bookings or not resource_map:
        logging.info("No bookings or resource map found in data. Setting all to ECO temp.")
        bookings = [] # Ensure we continue to step 4 to set ECO temp

    # Map resource IDs to location names for easier lookups
    location_map: Dict[str, str] = {
        str(resource_id): resource_info.get("name", "")
        for resource_id, resource_info in resource_map.items()
    }

    # 3. Determine required target times and temperatures
    target_schedules: Dict[str, Dict[str, Any]] = {}
    current_time_utc = datetime.datetime.now(pytz.utc)
    current_time_local = current_time_utc.astimezone(LOCAL_TZ)
    
    # Define the end of the day to limit the scope of bookings (e.g., next 24 hours)
    search_end_time = current_time_local + datetime.timedelta(hours=24) 

    for booking in bookings:
        resource_id = str(booking.get("resource_id"))
        resource_name = location_map.get(resource_id)

        # Skip if resource name is unknown or not configured
        if not resource_name or resource_name not in config["locations"]:
            logging.debug(f"Skipping booking for unconfigured resource ID: {resource_id}")
            continue

        try:
            start_dt_utc = dateutil.parser.parse(booking["start_datetime"]).astimezone(pytz.utc)
            end_dt_utc = dateutil.parser.parse(booking["end_datetime"]).astimezone(pytz.utc)
            start_dt_local = start_dt_utc.astimezone(LOCAL_TZ)
            end_dt_local = end_dt_utc.astimezone(LOCAL_TZ)

        except (KeyError, dateutil.parser.ParserError) as e:
            logging.error(f"Skipping booking due to time parsing error: {e} in {booking}")
            continue

        # Only process bookings starting soon and those currently active
        if start_dt_local > search_end_time:
            continue
            
        # Also skip bookings that have finished
        if end_dt_local < current_time_local:
            continue

        loc_config = config["locations"][resource_name]
        
        target_temp = config["default_temperature"]
        preheat_minutes = config["preheat_time_minutes"] 
        
        # Check if external temperature is below the minimum external temp threshold for this location
        min_temp_threshold = loc_config.get("min_external_temp", 5)
        
        # Only calculate complex pre-heat if it's cold enough (or if we had to use the 0.0 fallback)
        if external_temp < min_temp_threshold or external_temp == 0.0:
            preheat_minutes = calculate_preheat_minutes(
                target_temp=target_temp,
                # Current internal temp is hard to get fast, so we estimate a cold starting point (ECO_TEMPERATURE)
                current_temp=config["eco_temperature"], 
                heat_loss_factor=loc_config.get("heat_loss_factor", 1.0),
                external_temp=external_temp,
            )

        preheat_start_dt_local = start_dt_local - datetime.timedelta(minutes=preheat_minutes)
        
        # The key check: Is the calculated pre-heat start time now or in the past?
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

    # Identify all configured zones across all hubs
    for loc_config in config["locations"].values():
        hub_name = loc_config["neohub"]
        for zone in loc_config["zones"]:
             if zone not in all_neohub_zones[hub_name]:
                 all_neohub_zones[hub_name].append(zone)

    # Determine which zones need heat and at what temperature
    for schedule in target_schedules.values():
        hub_name = schedule["neohub"]
        temp = schedule["target_temp"]
        zones_for_schedule = schedule["zones"]
        
        zones_to_heat.setdefault(hub_name, {})
        
        for zone in zones_for_schedule:
            current_target = zones_to_heat[hub_name].get(zone)
            if current_target is None or temp > current_target:
                zones_to_heat[hub_name][zone] = temp
                
    # Apply ON schedules
    tasks = []
    for hub_name, zone_temps in zones_to_heat.items():
        for zone_name, temp in zone_temps.items():
            tasks.append(update_neohub_zone(hub_name, zone_name, temp))

    # Apply OFF schedules (ECO_TEMPERATURE) to zones not currently needed
    eco_temp = config["eco_temperature"]
    for hub_name, all_zones in all_neohub_zones.items():
        zones_heating = zones_to_heat.get(hub_name, {})
        for zone_name in all_zones:
            is_heating_needed = zone_name in zones_heating
            
            if not is_heating_needed:
                tasks.append(update_neohub_zone(hub_name, zone_name, eco_temp))

    # Run all NeoHub updates concurrently
    if tasks:
        await asyncio.gather(*tasks)

    logging.info("Heating schedule update complete.")

# --- Main Application Entry Point ---

def main():
    """Application entry point."""
    global config
    
    # Load configuration
    config = load_config()
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return
        
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # Initialize NeoHub connections
    for neohub_name, neohub_config in config["neohubs"].items():
        if not asyncio.run(connect_to_neohub(neohub_name, neohub_config)):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()
            
    # Optional: Log zones found on startup
    for neohub_name in config["neohubs"]:
        zones = asyncio.run(get_zones(neohub_name))
        if zones:
            logging.info(f"Zones on {neohub_name}: {zones}")
                
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit()
        
    # Create a scheduler.
    scheduler = BackgroundScheduler(timezone=LOCAL_TZ)

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    asyncio.run(update_heating_schedule())  # Run immediately
    scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
    scheduler.start()
    logging.info("Scheduler started. Running updates every 60 minutes.")

    try:
        # This keeps the main thread alive, which is necessary for the scheduler to run.
        while True:
            time.sleep(600)
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler when the user presses Ctrl+C or process is terminated
        logging.info("Shutting down scheduler...")
        scheduler.shutdown()
        
if __name__ == "__main__":
    main()
