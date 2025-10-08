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
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()  # Get logging level from env

# Neohub Configuration from Environment Variables
NEOHUBS = {}
neohub_count = 1
while True:
    neohub_name = os.environ.get(f"NEOHUB_{neohub_count}_NAME")
    neohub_address = os.environ.get(f"NEOHUB_{neohub_count}_ADDRESS")
    neohub_port = os.environ.get(f"NEOHUB_{neohub_count}_PORT", "4243")
    neohub_token = os.environ.get(f"NEOHUB_{neohub_count}_TOKEN")
    if not neohub_name or not neohub_address or not neohub_token:
        if neohub_count == 1:
            logging.warning(
                "No Neohub configuration found in environment variables.  Ensure NEOHUB_1_NAME, NEOHUB_1_ADDRESS, and NEOHUB_1_TOKEN are set."
            )
        break
    NEOHUBS[neohub_name] = {
        "address": neohub_address,
        "port": int(neohub_port),
        "token": neohub_token,
    }
    neohub_count += 1
if not NEOHUBS:
    logging.warning("No Neohub configurations were loaded.")
    # Global variables
neohub_connections = {}
config = None
hubs = {}  # Dictionary to store NeoHub instances

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOGGING_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def load_config(config_file: str) -> Optional[Dict[str, Any]]:
    """Loads configuration data from a JSON file."""
    try:
        with open(config_file, "r") as f:
            loaded_config = json.load(f)
            if "neohubs" in loaded_config:
                loaded_config["neohubs"].update(NEOHUBS)
            else:
                loaded_config["neohubs"] = NEOHUBS
            return loaded_config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_file}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON in {config_file}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


def connect_to_neohub(neohub_name: str, neohub_config: Dict[str, Any]) -> bool:
    """Connects to a Neohub using neohubapi."""
    global hubs
    try:
        # Use the port from the environment variable, defaulting to 4243
        port = neohub_config['port']
        token = neohub_config.get('token')  # Token is optional
        hub = NeoHub(host=neohub_config['address'], port=port, token=token)
        hubs[neohub_name] = hub  # Store
        logging.info(f"Connected to Neohub: {neohub_name} at {neohub_config['address']}:{port}")
        return True
    except (NeoHubConnectionError, NeoHubUsageError) as e:
        logging.error(f"Error connecting to Neohub {neohub_name}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False

def validate_config(config: Dict[str, Any]) -> bool:
    if "neohubs" not in config or not config["neohubs"]:
        logging.error("No Neohubs found in configuration.")
        return False
    if "locations" not in config or not config["locations"]:
        logging.error("No locations found in configuration.")
        return False
    return True

async def send_command(neohub_name: str, command: Dict[str, Any]) -> Optional[Any]:
    """Sends a command to the Neohub using neohubapi.

    If the command is a complex dictionary (like a STORE_PROFILE command), it is manually 
    serialized to a proper JSON string to prevent the library's internal serialization 
    from using single quotes and Python booleans (True/False), which the hub rejects.
    """
    global hubs
    hub = hubs.get(neohub_name)
    if hub is None:
        logging.error(f"Not connected to Neohub: {neohub_name}")
        return None
    
    # --- START FIX FOR INVALID JSON ERROR ---
    # The neohubapi's internal serialization of complex dicts often uses str() which produces 
    # single quotes and Python booleans (True/False) instead of JSON (double quotes, true/false).
    
    command_to_send: Any = command
    
    # Check for commands known to fail due to complex nested dicts/booleans
    if "STORE_PROFILE2" in command or "STORE_PROFILE" in command:
        # Manually serialize the inner command to a proper JSON string using double quotes and lowercase booleans
        command_to_send = json.dumps(command)
        
        # Now we rely on hub._send to take this string and wrap it correctly in the final 
        # WebSocket payload, escaping the inner quotes.
        
        logging.debug(f"DEBUG: Manually serialized command string for low-level send: {command_to_send}")
    # --- END FIX ---
        
    try:
        # Pass the (potentially serialized) command to the library's internal send method
        # The library should now see a string if it was serialized, or a simple dict otherwise.
        response = await hub._send(command_to_send)
        return response
    except (NeoHubUsageError, NeoHubConnectionError) as e:
        logging.error(f"Error sending command to Neohub {neohub_name}: {e}")
        return None
    except json.decoder.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response from Neohub {neohub_name}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Retrieves zone names from the Neohub using neohubapi."""
    logging.info(f"Getting zones from Neohub: {neohub_name}")
    command = {"GET_ZONES": 0}
    response = await send_command(neohub_name, command)
    if response:
        zones = []
        # The response is a NeoHubResponse object, its attributes are the zones
        if hasattr(response, '__dict__'):
             for attr_name in response.__dict__.keys():
                 zones.append(attr_name)
        return zones
    return None


async def set_temperature(neohub_name: str, zone_name: str, temperature: float) -> Optional[Dict[str, Any]]:
    """Sets the temperature for a specified zone using neohubapi."""
    logging.info(f"Setting temperature for zone {zone_name} on Neohub {neohub_name} to {temperature}")
    command = {"SET_TEMP": [temperature, zone_name]}
    response = await send_command(neohub_name, command)
    return response #modified


async def get_live_data(neohub_name: str) -> Optional[Dict[str, Any]]:
    """Gets the live data using neohubapi."""
    logging.info(f"Getting live data from Neohub: {neohub_name}")
    command = {"GET_LIVE_DATA": 0}
    response = await send_command(neohub_name, command)
    return response


async def store_profile(neohub_name: str, profile_name: str, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Stores a heating profile on the Neohub using neohubapi."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")
    command = {"STORE_PROFILE": {"name": profile_name, "info": profile_data}}
    # FIX: Pass the dict directly, this is now handled by the fix in send_command
    response = await send_command(neohub_name, command)  
    return response

async def store_profile2(neohub_name: str, profile_name: str, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Stores a heating profile on the Neohub, passing a Python dictionary structure directly to avoid double-encoding."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")

    # 1. CREATE THE COMMAND PAYLOAD (clean Python dict with float temps and P_TYPE)
    # The structure is exactly what the Hub expects under the main COMMAND key.
    inner_payload = {
        "STORE_PROFILE2": {
            "name": profile_name,
            "P_TYPE": 0, # 0 for Heating Profile
            "info": profile_data
        }
    }

    # 2. DEBUGGING ECHO
    logging.debug(f"DEBUG: FINAL Python Dict Payload: {json.dumps(inner_payload)}")

    # 3. SEND THE COMMAND DICT DIRECTLY
    # The fix is now in send_command, which detects "STORE_PROFILE2" and serializes the dict
    # to a proper JSON string before handing it to the low-level library transport.
    response = await send_command(neohub_name, inner_payload)
    return response

async def get_profile(neohub_name: str, profile_name: str) -> Optional[Dict[str, Any]]:
    """Retrieves a heating profile from the Neohub using neohubapi."""
    logging.info(f"Getting profile {profile_name} from Neohub {neohub_name}")
    command = {"GET_PROFILE": profile_name}
    response = await send_command(neohub_name, command)
    return response


async def get_neohub_firmware_version(neohub_name: str) -> Optional[int]:
    """Gets the firmware version of the Neohub."""
    logger = logging.getLogger("neohub")
    logger.info(f"Getting firmware version from Neohub: {neohub_name}")

    # Construct the FIRMWARE command
    command = {"FIRMWARE": 0}

    # Get Neohub configuration
    neohub_config = config["neohubs"].get(neohub_name)
    if not neohub_config:
        logger.error(f"Neohub configuration not found for {neohub_name}")
        return None

    # Get the Neohub instance
    global hubs
    hub = hubs.get(neohub_name)
    if hub is None:
        logging.error(f"Not connected to Neohub: {neohub_name}")
        return None

    try:
        # Use the neohubapi library's _send function directly
        response = await hub._send(command)

        if response:
            try:
                # Extract the firmware version from the response
                # Use .get() and safe conversion as it's a NeoHubResponse object
                firmware_version = int(getattr(response, "HUB_VERSION", 0))
                logger.info(f"Firmware version for Neohub {neohub_name}: {firmware_version}")
                return firmware_version
            except (ValueError, AttributeError) as e:
                logger.error(f"Error parsing firmware version from response: {e}")
                return None
        else:
            logger.error(f"Failed to retrieve system data from Neohub {neohub_name}.")
            return None

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return None

async def apply_profile_to_zones(neohub_name: str, zones: List[str], profile_name: str):
    """
    Applies a stored profile (by name) to a list of specified zones and sets them to 'Timed' mode.
    Note: The NeoHub typically assigns profiles using an internal ID. Since we only have the name, 
    we are skipping the SET_PROFILE step (which requires the ID) and only setting the mode to Timed.
    """
    logging.info(f"Attempting to apply profile '{profile_name}' to zones: {zones} on Neohub {neohub_name}")
    
    # In a full implementation, you'd fetch all profiles to find the profile_id by name.
    # For now, we assume the profile is created and focus on setting the mode.

    # Iterate through zones and set them to Program/Timed mode
    for zone_name in zones:
        # Setting mode to 1 typically means "Program/Timed" mode for neoStats
        command = {"SET_MODE": [1, zone_name]} 
        response = await send_command(neohub_name, command)
        
        if response:
            logging.info(f"Successfully set zone '{zone_name}' to Timed/Program mode.")
        else:
            logging.error(f"Failed to set zone '{zone_name}' to Timed/Program mode. Hub response: {response}")


def get_external_temperature() -> Optional[float]:
    """Gets the current external temperature."""
    try:
        response = requests.get(
            f"https://api.openweathermap.org/data/2.5/weather?q={OPENWEATHERMAP_CITY}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
        )
        response.raise_for_status()
        data = response.json()
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(
                f"get_external_temperature:  Temp from OpenWeatherMap: {data['main']['temp']}"
            )
        return data["main"]["temp"]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching external temperature: {e}")
        return None
    except KeyError:
        logging.error("Unexpected response format from OpenWeatherMap")
        return None



def get_json_data(url: str) -> Optional[Dict[str, Any]]:
    """Fetches JSON data from a given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"get_json_data: Got data from {url}: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching JSON data from {url}: {e}")
        return None



def get_bookings_and_locations() -> Optional[Dict[str, Any]]:
    """Fetches bookings and locations data from ChurchSuite."""
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"get_bookings_and_locations: Fetching data from {CHURCHSUITE_URL}")
    return get_json_data(CHURCHSUITE_URL)



def calculate_schedule(
    booking: Dict[str, Any], config: Dict[str, Any], external_temperature: Optional[float], resource_map: Dict[int, str]
) -> Optional[Dict[str, Any]]:
    """Calculates the heating schedule for a single booking."""
    resource_id = booking["resource_id"]
    location_name = resource_map.get(resource_id)
    if not location_name:
        logging.error(f"Resource ID '{resource_id}' not found in resource map.")
        return None

    if location_name not in config["locations"]:
        logging.error(f"Location '{location_name}' not found in configuration.")
        return None

    location_config = config["locations"][location_name]
    neohub_name = location_config["neohub"]
    zones = location_config["zones"]
    heat_loss_factor = location_config["heat_loss_factor"]
    min_external_temp = location_config["min_external_temp"]

    start_time_str = booking.get("starts_at")
    end_time_str = booking.get("ends_at")

    if not start_time_str or not end_time_str:
        logging.error(f"Booking is missing start or end time: {booking}")
        return None
    # Use dateutil.parser.parse to handle the timestamp format
    start_time = dateutil.parser.parse(start_time_str).replace(tzinfo=None)
    end_time = dateutil.parser.parse(end_time_str).replace(tzinfo=None)
    preheat_time = datetime.timedelta(minutes=PREHEAT_TIME_MINUTES)
    if (
        external_temperature is not None
        and external_temperature < TEMPERATURE_SENSITIVITY
        and external_temperature < min_external_temp
    ):
        temp_diff = TEMPERATURE_SENSITIVITY - external_temperature
        adjustment = (
            temp_diff * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE
        ) * heat_loss_factor
        preheat_time += datetime.timedelta(minutes=adjustment)
        logging.info(
            f"Adjusted preheat time for {location_name} by {adjustment:.0f} minutes due to external temperature."
        )
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(
                f"calculate_schedule: Adjusted preheat time for {location_name} by {adjustment:.0f} minutes.  External temp = {external_temperature}, temp_diff = {temp_diff}, heat_loss_factor={heat_loss_factor}, preheat_time={preheat_time}"
            )
            
    profile_data = {}
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    
    # Define time offsets for creating unique chronological event times
    time_delta_1 = datetime.timedelta(minutes=1)
    time_delta_2 = datetime.timedelta(minutes=2)

    def add_level(
        day_data: Dict[str, Any],
        level_name: str,
        event_time: datetime.datetime,
        temperature: float,
    ):
        """Adds a level to the day's schedule."""
        # Note: The 'mode' is 5 for Heatmiser neoStat, 0 for neoAir. We use 5 as default.
        # The boolean MUST be a Python boolean (True/False) which will be serialized to JSON 'true'/'false'
        day_data[level_name] = [
            event_time.strftime("%H:%M"),
            float(temperature),  # Ensure float
            5,  # Mode (5)
            True,  # Comfort mode
        ]

    for day in days:
        # Initialize the day's schedule dictionary correctly
        profile_data[day] = {}
        day_schedule = profile_data[day]
        
        # Heating ON block (based on booking start time minus preheat)
        on_time = start_time - preheat_time
        
        # Heating OFF block (based on booking end time)
        off_time = end_time
        
        # Ensure the times in the profile are sequential/chronological for the NeoHub API
        # The NeoHub profile format requires 6 levels to be present, even if unused.

        # Level 1: Wake (Heating ON Start)
        add_level(day_schedule, "wake", on_time, DEFAULT_TEMPERATURE) 
        
        # Level 2: Level2 (ON Start + 1 min)
        add_level(day_schedule, "level2", on_time + time_delta_1, DEFAULT_TEMPERATURE) 
        
        # Level 3: Level1 (ON Start + 2 min)
        add_level(day_schedule, "level1", on_time + time_delta_2, DEFAULT_TEMPERATURE) 
        
        # Level 4: Level3 (Heating OFF Start)
        add_level(day_schedule, "level3", off_time, ECO_TEMPERATURE)
        
        # Level 5: Level4 (OFF Start + 1 min)
        add_level(day_schedule, "level4", off_time + time_delta_1, ECO_TEMPERATURE)
        
        # Level 6: Sleep (OFF Start + 2 min)
        add_level(day_schedule, "sleep", off_time + time_delta_2, ECO_TEMPERATURE) 

    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"calculate_schedule: Calculated schedule: {profile_data}")
    return profile_data


async def update_heating_schedule():
    """Main function to orchestrate fetching data and applying heating schedules."""
    logging.info("--- Starting heating schedule update ---")

    # 1. Fetch external data
    external_temperature = get_external_temperature()
    bookings_and_locations = get_bookings_and_locations()
    
    if not bookings_and_locations:
        logging.error("Failed to fetch bookings data. Aborting update.")
        return

    # Extract bookings and resource map
    bookings = bookings_and_locations.get("bookings", [])
    resource_map = {
        resource["id"]: resource["name"]
        for resource in bookings_and_locations.get("resources", [])
    }
    
    if not bookings or not resource_map:
        logging.warning("No bookings or resource map found in data. Aborting update.")
        return

    # Map to track which profiles have been created/updated per hub (to avoid redundant creation)
    updated_profiles = {} 

    for booking in bookings:
        try:
            # 2. Calculate the new schedule profile
            profile_data = calculate_schedule(
                booking=booking, 
                config=config, 
                external_temperature=external_temperature, 
                resource_map=resource_map
            )
            
            if profile_data is None:
                continue

            resource_id = booking["resource_id"]
            location_name = resource_map.get(resource_id)
            if not location_name or location_name not in config["locations"]:
                continue # Already logged inside calculate_schedule

            loc_config = config["locations"][location_name]
            neohub_name = loc_config["neohub"]
            zone_names = loc_config["zones"]
            profile_name = "Current Week" # Using "Current Week" as seen in the logs
            
            # --- START: Schedule Assignment Logic ---
            
            # Check if we already stored the profile for this hub in this run
            if neohub_name not in updated_profiles:
                # 3. Store the profile on the hub (this is the command that was failing)
                store_response = await store_profile2(
                    neohub_name=neohub_name,
                    profile_name=profile_name,
                    profile_data=profile_data,
                )
                
                if store_response:
                    logging.info(f"Successfully stored profile '{profile_name}' on {neohub_name}.")
                    updated_profiles[neohub_name] = True
                else:
                    logging.error(f"Failed to store profile '{profile_name}' on {neohub_name}. Aborting zone application.")
                    continue

            # 4. Apply the profile to the zones associated with this location
            await apply_profile_to_zones(
                neohub_name=neohub_name,
                zones=zone_names,
                profile_name=profile_name,
            )
            
            logging.info(f"Applied heating schedule for booking at '{location_name}' starting at {booking.get('starts_at')}")

            # --- END: Schedule Assignment Logic ---

        except KeyError as e:
            logging.error(f"Missing key in booking data: {e} in {booking}")
            continue
        except Exception as e:
            logging.exception(f"Unhandled error processing booking: {e}")
            continue

    logging.info("--- Heating schedule update finished ---")


# --- Main Execution ---

def main():
    """The main entry point of the application."""
    global config
    
    # Load and validate configuration
    config_data = load_config(CONFIG_FILE)
    if config_data is None:
        logging.error("Failed to load configuration. Exiting.")
        return
    config = config_data # Store globally

    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # Initialize NeoHub connections
    for neohub_name, neohub_config in config.get("neohubs", {}).items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()
            
    # Optional: Fetch and log zones (for debug/info)
    for neohub_name in config.get("neohubs", {}):
        try:
            zones = asyncio.run(get_zones(neohub_name))
            if zones:
                logging.info(f"Zones on {neohub_name}: {zones}")
                if LOGGING_LEVEL == "DEBUG":
                    logging.debug(f"main: Zones on {neohub_name}: {zones}")
        except Exception as e:
             logging.error(f"Error getting zones for {neohub_name}: {e}")

    # This check needs to be placed after config is loaded but before it's used to connect.
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        exit()


    # Create a scheduler.
    scheduler = BackgroundScheduler()

    # Run update_heating_schedule() immediately, and then schedule it to run every 60 minutes.
    try:
        asyncio.run(update_heating_schedule())  # Run immediately
            
        scheduler.add_job(lambda: asyncio.run(update_heating_schedule()), "interval", minutes=60)
        scheduler.start()
    except Exception as e:
        logging.error(f"Error during initial update or scheduler start: {e}")
        return

    try:
        while True:
            time.sleep(600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
