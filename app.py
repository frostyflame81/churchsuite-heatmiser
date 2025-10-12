import asyncio
import datetime
import json
import logging
import time
import itertools
import requests # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler # type: ignore
import argparse
import os
import pytz # type: ignore
import dateutil.parser # type: ignore
from typing import Dict, Any, List, Optional, Union
import websockets # type: ignore
import ssl
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError, WebSocketClient # type: ignore

_command_id_counter = itertools.count(start=100)

# Configuration
CHURCHSUITE_TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE")
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

def get_hub(neohub_name: str) -> Optional[NeoHub]:
    """Retrieves the connected NeoHub object by name."""
    global hubs # Use the correct global variable
    hub = hubs.get(neohub_name)
    if not hub:
        logging.error(f"NeoHub '{neohub_name}' not connected or found in cache.")
    return hub

def validate_config(config: Dict[str, Any]) -> bool:
    if "neohubs" not in config or not config["neohubs"]:
        logging.error("No Neohubs found in configuration.")
        return False
    if "locations" not in config or not config["locations"]:
        logging.error("No locations found in configuration.")
        return False
    return True

async def send_command(neohub_name: str, command: Dict[str, Any]) -> Optional[Any]:
    """
    Sends a command to the Neohub, using a custom raw send for complex profile commands.
    """
    global hubs
    hub = hubs.get(neohub_name)
    if hub is None:
        logging.error(f"Not connected to Neohub: {neohub_name}")
        return None

    # --- START FIX: Custom raw send for complex commands ---
    is_profile_command = False
    if isinstance(command, dict):
        for key in ["STORE_PROFILE", "STORE_PROFILE2", "GET_PROFILE"]:
            if key in command and isinstance(command[key], (dict, str)): # Added GET_PROFILE to handle name check
                is_profile_command = True
                break
            
    if is_profile_command:
        # Bypass the broken hub._send() for profile commands, use the new raw sender
        return await _send_raw_command(hub, command, "RAW_PROFILE_CMD") # <<-- MODIFIED LINE
    # --- END FIX ---
    
    # Normal command handling (for simple commands like GET_ZONES)
    try:
        response = await hub._send(command)
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
        for attr_name, attr_value in vars(response).items():
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
    command_json = json.dumps(command)  # Convert the command to a JSON string
    response = await send_command(neohub_name, command_json)  # Pass the JSON string to send_command
    return response

async def store_profile2(neohub_name: str, profile_name: str, schedule_data: Dict[str, Any]) -> Optional[int]:
    """
    Manages profile creation and updating, using GET_PROFILE to find the required ID.
    Returns the profile ID on success.
    """
    hub = get_hub(neohub_name)  # Assuming a global or passed function to get the hub object
    
    # 1. Check for existing profile ID
    profile_id = await get_profile_id_by_name(hub, neohub_name, profile_name)

    # 2. Build the STORE_PROFILE2 command structure
    command_payload = {
        "name": profile_name,
        "P_TYPE": 0,
        "info": schedule_data
    }
    
    # 3. Conditionally add the PROFILE_ID for updating
    if profile_id is not None:
        command_payload["PROFILE_ID"] = profile_id
        logging.info(f"Preparing to UPDATE profile '{profile_name}' with ID {profile_id} on {neohub_name}.")
    else:
        logging.info(f"Preparing to CREATE new profile '{profile_name}' on {neohub_name}.")

    final_command = {"STORE_PROFILE2": command_payload}
    
    # 4. Send the command
    response_dict = await _send_raw_command(hub, final_command, f"STORE_PROFILE2:{profile_name}")

    if not response_dict:
        logging.error(f"Failed to receive a response for STORE_PROFILE2 for {profile_name}.")
        return None

    # 5. Parse the success/failure response
    response_inner_str = response_dict.get("response", "{}")
    
    # Handle the "No such ID or name already exists" error (the expected outcome if the logic fails)
    if response_dict.get("error") and "No such ID or name already exists" in response_dict["error"]:
        logging.critical(f"Profile management error: Profile '{profile_name}' failed. This should not happen if the ID check worked.")
        return None
    
    try:
        inner_response = json.loads(response_inner_str)
    except json.JSONDecodeError:
        logging.error(f"Failed to parse inner STORE_PROFILE2 response for {profile_name}.")
        return None
    
    # A successful response returns '{"ID":<id>,"result":"profile created"}' for creation
    # For an update, the result might be less verbose, but the core success path is fine.
    
    if inner_response.get("result") == "profile created" or inner_response.get("result") == "profile updated":
        # On creation, the new ID is returned in 'ID'. On update, the existing ID is used.
        final_id = inner_response.get("ID", profile_id) # Use returned ID or the ID we passed
        logging.info(f"Profile '{profile_name}' successfully stored/updated with ID: {final_id}")
        return final_id
    else:
        logging.error(f"STORE_PROFILE2 command returned unexpected result: {inner_response}")
        return None

async def get_profile_id_by_name(hub: NeoHub, neohub_name: str, profile_name: str) -> Optional[int]:
    """
    Calls GET_PROFILE to find the profile ID for a given name.
    """
    command = {"GET_PROFILE": profile_name}
    
    # Use the new unified sender
    response_dict = await _send_raw_command(hub, command, f"GET_PROFILE:{profile_name}")

    if not response_dict or "error" in response_dict:
        logging.warning(f"Error checking profile ID for '{profile_name}' on {neohub_name}: {response_dict}")
        return None

    # Hub returns a JSON string inside the 'response' key
    response_inner_str = response_dict.get("response", "{}")
    try:
        # Attempt to parse the inner JSON string
        inner_response = json.loads(response_inner_str)
    except json.JSONDecodeError:
        logging.warning(f"Failed to parse inner GET_PROFILE response for {profile_name}.")
        return None

    # Check for the presence of PROFILE_ID on success (API PDF format)
    profile_id = inner_response.get("PROFILE_ID")
    if isinstance(profile_id, int):
        logging.info(f"Found existing profile '{profile_name}' with ID: {profile_id}")
        return profile_id
    
    # If no ID is found (profile does not exist), this is the intended path for creation
    logging.info(f"Profile '{profile_name}' not found on {neohub_name}. Will be created.")
    return None

async def activate_profile_on_zone(neohub_name: str, zone_name: str, profile_id: int):
    hub = get_hub(neohub_name)
    
    # 1. Get the NeoStat device object for the zone name
    try:
        # Assuming hub.get_devices_data() is the source of all devices/zones
        devices_data = await hub.get_devices_data()
        
        # Find the device/zone that matches the name
        device_to_activate = None
        for device in devices_data.get('neo_devices', []):
            if device.name == zone_name:
                device_to_activate = device
                break
        
        if not device_to_activate:
            logging.error(f"Zone/Device '{zone_name}' not found on {neohub_name}.")
            return False

        # 2. Call the library function to set the profile ID
        logging.info(f"Activating profile ID {profile_id} on zone '{zone_name}'...")
        
        # The library's NeoStat object has a set_profile_id method
        response = await device_to_activate.set_profile_id(profile_id)
        
        # NOTE: You will need to inspect the 'response' structure to confirm success 
        # as the library abstracts the raw WebSocket response here.
        logging.info(f"Profile activation command sent for {zone_name}. Response: {response}")
        return True

    except Exception as e:
        logging.error(f"Error during profile activation for zone {zone_name}: {e}")
        return False

async def _send_raw_command(hub: NeoHub, command: Dict[str, Any], command_name: str) -> Optional[Dict[str, Any]]:
    """
    Manually constructs, sends, and waits for the response for any raw command.
    Uses the necessary escaping hacks for the command payload.
    Returns the final parsed response dictionary from the hub.
    """
    global _command_id_counter
    
    hub_token = getattr(hub, '_token', None)
    hub_client = getattr(hub, '_client', None)
    
    if not hub_token or not hub_client:
        logging.error("Could not access private token or client (_token or _client) for raw send.")
        return None

    try:
        command_to_send = command
        
        # 1. Serialize the command (retaining the unquoted boolean format for STORE_PROFILE2)
        command_id = next(_command_id_counter)
        command_value_str = json.dumps(command_to_send, separators=(',', ':'))

        # 2. HACK 1: Convert all double quotes to single quotes
        command_value_str_hacked = command_value_str.replace('"', "'")
        
        # 3. HACK 2: Manually construct the INNER_MESSAGE string
        message_str = (
            '{\\"token\\": \\"' + hub_token + '\\", '
            '\\"COMMANDS\\": ['
                '{\\"COMMAND\\": \\"' + command_value_str_hacked + '\\", '
                '\\"COMMANDID\\": ' + str(command_id) + '}'
            ']}'
        )

        # 4. Construct the final payload dictionary (outer wrapper)
        final_payload_dict = {
            "message_type": "hm_get_command_queue",
            "message": message_str 
        }
        
        # 5. Final Serialization & Escaping Hacks
        final_payload_string = json.dumps(final_payload_dict) 
        final_payload_string = final_payload_string.replace('\\\\\\"', '\\"')
        
        # 6. Send and wait
        raw_connection = getattr(hub_client, '_websocket', None)
        raw_ws_send = getattr(raw_connection, 'send', None) if raw_connection else None
        pending_requests = getattr(hub_client, '_pending_requests', None)
        request_timeout = getattr(hub_client, '_request_timeout', 60) 
        
        future: asyncio.Future[Any] = asyncio.Future()
        pending_requests[command_id] = future

        logging.debug(f"Raw Sending ({command_name}): {final_payload_string}")
        
        await raw_ws_send(final_payload_string)
        response_dict = await asyncio.wait_for(future, timeout=request_timeout)
        
        logging.debug(f"Received {command_name} response (COMMANDID {command_id}): {response_dict}")

        return response_dict

    except asyncio.TimeoutError:
        logging.error(f"Timeout waiting for response for command {command_id}.")
        return {"command_id": command_id, "status": "Timeout"}
    except Exception as e:
        logging.error(f"Error during raw WebSocket send/receive for {command_name}: {e}")
        return None
    finally:
        if pending_requests and 'command_id' in locals() and command_id in pending_requests:
            del pending_requests[command_id]

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

    # Construct the GET_SYSTEM command
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
                firmware_version = int(response.get("HUB_VERSION"))
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

async def get_external_temperature() -> float:
    """Retrieves the current external temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY or not OPENWEATHERMAP_CITY:
        logging.warning("OpenWeatherMap API key or city is not configured. Defaulting to 0°C.")
        return 0.0 # Default to the worst case for safety
    
    # Using the 'weather' endpoint for simple current temperature retrieval
    url = (
        f"http://api.openweathermap.org/data/2.5/weather?"
        f"q={OPENWEATHERMAP_CITY}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    )
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check for successful data retrieval
        if data and data.get("main"):
            current_temp = data["main"]["temp"]
            logging.info(f"External temperature in {OPENWEATHERMAP_CITY}: {current_temp}°C")
            return float(current_temp)
            
        logging.error("OpenWeatherMap response missing 'main' data.")
        return 0.0
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching external temperature: {e}")
        return 0.0

def get_json_data(url: str) -> Optional[Dict[str, Any]]:
    """Generic function to fetch JSON data from a URL."""
    try:
        if not url:
            logging.error("JSON data URL is not set.")
            return None
            
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching JSON data from {url}: {e}")
        return None

def _fetch_raw_churchsuite_data() -> Optional[List[Dict[str, Any]]]:
    """
    Internal helper to fetch raw JSON data from ChurchSuite and extract the 
    'booked_resources' list.
    """
    if LOGGING_LEVEL == "DEBUG": 
        logging.debug(f"_fetch_raw_churchsuite_data: Fetching data from {CHURCHSUITE_URL}")
        
    raw_data = get_json_data(CHURCHSUITE_URL)
    
    # Check for successful data fetch and the correct key
    if not raw_data or "booked_resources" not in raw_data:
        logging.error("Failed to fetch ChurchSuite data or 'booked_resources' key is missing.")
        return None
        
    # Return the list of resources/bookings
    return raw_data.get("booked_resources", [])

def get_bookings_and_locations() -> Dict[tuple[str, str], List[Dict[str, Any]]]:
    """
    Fetches raw resources/bookings and aggregates them into a map keyed by the 
    required (NeoHub, Zone) pair. This prepares the data for profile generation.
    
    Returns: { (neohub_name, zone_name): [raw_resource_dict1, raw_resource_dict2, ...] }
    """
    global config
    raw_resources = _fetch_raw_churchsuite_data()

    if not raw_resources or not config:
        logging.warning("No raw resources fetched or configuration is missing.")
        return {}

    # Key: (neohub_name, zone_name) -> Value: List[raw_resource_dict]
    consolidated_bookings: Dict[tuple[str, str], List[Dict[str, Any]]] = {}

    for resource in raw_resources:
        # Assuming the ChurchSuite resource structure places the name in the 'resource' key
        location_name = resource.get("resource")
        
        # 1. Find the corresponding location configuration
        location_config = config["locations"].get(location_name)

        if not location_config:
            logging.warning(f"Resource location '{location_name}' not found in config. Skipping resource.")
            continue

        neohub_name = location_config["neohub"]

        # 2. A single resource affects multiple zones (e.g., Chancel affects "Main Church" and "Raphael Room...")
        for zone_name in location_config["zones"]:
            key = (neohub_name, zone_name)

            # 3. Add the raw resource/booking to the consolidated list for this hub/zone
            if key not in consolidated_bookings:
                consolidated_bookings[key] = []
            consolidated_bookings[key].append(resource)

    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"get_bookings_and_locations: Consolidated {len(raw_resources)} raw resources into {len(consolidated_bookings)} Hub/Zone profiles.")

    return consolidated_bookings

async def activate_profile_on_zone(neohub_name: str, zone_name: str, profile_id: int) -> bool:
    """
    Applies the given profile ID to the thermostat device corresponding to the zone_name.
    """
    hub = get_hub(neohub_name)
    if not hub:
        return False
    
    try:
        # 1. Fetch all devices to find the NeoStat object by name
        hub_data = await hub.get_devices_data()
        
        # The NeoHub device list contains NeoStat objects which have the set_profile_id method
        device_to_activate = None
        for device in hub_data.get('neo_devices', []):
            if device.name == zone_name:
                device_to_activate = device
                break
        
        if not device_to_activate:
            logging.error(f"Zone/Device '{zone_name}' not found on {neohub_name} during activation.")
            return False

        # 2. Call the library function to set the profile ID
        logging.info(f"Activating profile ID {profile_id} on zone '{zone_name}'...")
        
        # The NeoStat object's method internally calls SET_PROFILE_ID on the hub
        response = await device_to_activate.set_profile_id(profile_id)
        
        # NOTE: The library handles the response. Logging the result is the best we can do.
        logging.info(f"Profile activation command sent for {zone_name}. Response: {response}")
        return True

    except Exception as e:
        logging.error(f"Error during profile activation for zone {zone_name}: {e}")
        return False

def _create_default_schedule() -> Dict[str, Any]:
    """Creates a default 7-day schedule dictionary (all levels set to ECO/OFF)."""
    # [Time "HH:MM", Temperature (float), Sensitivity (int), Heat_on (true/false)]
    default_level = ["00:00", ECO_TEMPERATURE, TEMPERATURE_SENSITIVITY, False]
    
    # Initialize all 6 levels to the default time
    day_schedule = {
        "wake": default_level.copy(), "level1": default_level.copy(), 
        "level2": default_level.copy(), "level3": default_level.copy(), 
        "level4": default_level.copy(), "sleep": default_level.copy()
    }
    
    # Initialize all 7 days with the default schedule
    schedule = {}
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
        schedule[day] = day_schedule.copy()
        
    return schedule

def _map_events_to_levels(events_for_day: List[Dict[str, Any]]) -> Dict[str, List[Union[str, float, int, bool]]]:
    """
    Takes a sorted list of consolidated heating events for one day and maps them 
    to the 6 rigid NeoHub schedule levels (wake, level1-4, sleep).

    We use the 6 levels as the start of heating periods.
    """
    day_levels = _create_default_schedule().get("monday", {}) # Get a fresh set of 6 levels
    
    # Sort events by start time
    sorted_events = sorted(events_for_day, key=lambda x: x['start_time'])
    
    # Map the first 5 unique start events to the first 5 available levels
    level_names = ["wake", "level1", "level2", "level3", "level4"]
    
    for i, event in enumerate(sorted_events):
        if i >= len(level_names):
            # We can only map up to 5 start events. The 6th level is reserved for 'sleep'.
            logging.warning(f"Exceeded 5 unique heating events for a day. Dropping event starting at {event['start_time'].strftime('%H:%M')}")
            break
            
        level_name = level_names[i]
        
        # Format: [Time "HH:MM", Temperature (float), Sensitivity (int), Heat_on (true/false)]
        day_levels[level_name] = [
            event['start_time'].strftime("%H:%M"),
            event['target_temp'],
            TEMPERATURE_SENSITIVITY,
            True # Always True for a heating start event
        ]

    # Handle the final 'sleep' event. This should be the earliest time ALL required heating
    # has ended, plus a 15-minute buffer, set back to ECO_TEMPERATURE/OFF.
    if sorted_events:
        last_event = sorted_events[-1]
        
        # Calculate when the last heat is no longer required (end time + 15 min buffer)
        sleep_time = last_event['end_time'] + datetime.timedelta(minutes=15)
        
        # Set the sleep level to the calculated time (or midnight if later)
        day_levels["sleep"] = [
            sleep_time.strftime("%H:%M"),
            ECO_TEMPERATURE,
            TEMPERATURE_SENSITIVITY,
            False # Heat off
        ]
        
    return day_levels

async def calculate_schedule(neohub_name: str, zone_name: str, raw_bookings: List[Dict[str, Any]], external_temp: float) -> Dict[str, Any]:
    """
    Calculates the consolidated heating profile for a single zone, applying preheat,
    temperature adjustments, and consolidation logic over a 7-day rolling window.
    
    Returns the final STORE_PROFILE2 'info' dictionary structure.
    """
    global config
    
    # Time setup
    tz = pytz.timezone(CHURCHSUITE_TIMEZONE)
    now_in_tz = datetime.datetime.now(tz)
    
    # The schedule must cover the next 7 days, rolling over the week boundary.
    end_of_window = now_in_tz + datetime.timedelta(days=7)
    
    # 1. Initialize the final 7-day schedule (key: 'monday', 'tuesday', etc.)
    final_schedule = _create_default_schedule()
    
    # A temporary structure to hold all *calculated* heating events before mapping
    # Key: day_name (e.g., 'monday') -> List of events: [{'start_time': dt, 'end_time': dt, 'target_temp': float}]
    daily_events: Dict[str, List[Dict[str, Any]]] = {day: [] for day in final_schedule.keys()}

    # 2. Get the full config for all locations that feed this zone
    zone_configs = []
    for loc_name, loc_config in config["locations"].items():
        if loc_config["neohub"] == neohub_name and zone_name in loc_config["zones"]:
            zone_configs.append((loc_name, loc_config))

    # 3. Process each raw booking
    for booking in raw_bookings:
        location_name = booking.get("resource") # Use the 'resource' key
        start_time_str = booking.get("start")
        end_time_str = booking.get("end")

        try:
            # Find the specific config for the location that generated this booking
            loc_config = next(c for name, c in zone_configs if name == location_name)
        except StopIteration:
            continue # Should not happen, but safe to skip

        # Parse and localize times
        event_start = dateutil.parser.parse(start_time_str).astimezone(tz)
        event_end = dateutil.parser.parse(end_time_str).astimezone(tz)
        
        # --- 4. 7-Day Window Check ---
        # We only care about events that START within the next 7 days.
        if event_start < now_in_tz or event_start >= end_of_window:
            logging.debug(f"Skipping booking {location_name} @ {event_start}: Outside 7-day rolling window.")
            continue
            
        # --- 5. Pre-Heat and Temp Calculation ---
        required_temp = DEFAULT_TEMPERATURE
        preheat_delta = datetime.timedelta(minutes=0)
        
        if external_temp >= loc_config["min_external_temp"]:
            # External temp is warm enough, no pre-heat or high temp required
            required_temp = ECO_TEMPERATURE 
        else:
            # Calculate required pre-heat time
            temp_diff = required_temp - external_temp
            preheat_minutes = (
                PREHEAT_TIME_MINUTES + 
                (temp_diff * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE * loc_config["heat_loss_factor"])
            )
            preheat_delta = datetime.timedelta(minutes=max(0, preheat_minutes))

        # Only create an event if active heating is required (above ECO)
        if required_temp > ECO_TEMPERATURE:
            
            # The actual time the heating must START to achieve target temp by event_start
            heating_start_time = event_start - preheat_delta
            
            day_name = heating_start_time.strftime('%A').lower()
            
            new_event = {
                'start_time': heating_start_time,
                'end_time': event_end, # Actual event end time
                'target_temp': required_temp,
                'location': location_name
            }
            
            daily_events[day_name].append(new_event)
            logging.debug(f"Event for {zone_name} on {day_name}: Heat start at {heating_start_time.strftime('%H:%M')} for {required_temp}°C (Booked: {event_start.strftime('%H:%M')})")

    # --- 6. Consolidate and Map Events to Final Schedule ---
    
    for day_name, events_for_day in daily_events.items():
        if not events_for_day:
            continue
            
        # Simplistic Consolidation: Merge events that start within 30 minutes of each other.
        # This prevents using multiple fixed NeoHub levels for close events.
        
        consolidated_day_events: List[Dict[str, Any]] = []
        
        # Sort events by start time to process them sequentially
        events_for_day.sort(key=lambda x: x['start_time'])
        
        for event in events_for_day:
            if not consolidated_day_events:
                # Start the first consolidated event
                consolidated_day_events.append(event)
                continue
                
            last_consolidated = consolidated_day_events[-1]
            
            # Check if the current event is close enough to the last one to merge (30 min buffer)
            # OR if the new event starts before the last one ends (full overlap)
            if event['start_time'] - last_consolidated['start_time'] < datetime.timedelta(minutes=30) or \
               event['start_time'] < last_consolidated['end_time']:
                
                # MERGE: Take the earliest start, latest end, and highest temp
                last_consolidated['start_time'] = min(last_consolidated['start_time'], event['start_time'])
                last_consolidated['end_time'] = max(last_consolidated['end_time'], event['end_time'])
                last_consolidated['target_temp'] = max(last_consolidated['target_temp'], event['target_temp'])
            else:
                # Not close, start a new consolidated event
                consolidated_day_events.append(event)
        
        # Map the consolidated events to the 6 fixed NeoHub levels
        final_schedule[day_name] = _map_events_to_levels(consolidated_day_events)

    return final_schedule

# Custom command to send a profile command using websockets directly
async def send_profile_command(
    neohub_name: str, command: Dict[str, Any], token: str, host: str, port: int
) -> Optional[Any]:
    """Sends a profile command to the Neohub using websockets directly."""
    logger = logging.getLogger("neohub")
    websocket = None  # Initialize websocket outside the try block
    try:
        uri = f"wss://{host}:{port}"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        websocket = await websockets.connect(uri, ssl=context)
        logger.debug("WebSocket connected successfully")

        # Construct the message without double JSON encoding
        message = {
            "message_type": "hm_get_command_queue",
            "message": json.dumps(
                {
                    "token": token,
                    "COMMANDS": [{"COMMAND": command, "COMMANDID": 1}],
                }
            ),
        }
        encoded_message = json.dumps(message)

        logger.debug("Sending: %s", encoded_message)
        await websocket.send(encoded_message)

        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=10)
            logger.debug("Received: %s", response)

            result = json.loads(response)
            if result.get("message_type") == "hm_set_command_response":
                return result["response"]
            else:
                logger.error(f"Unexpected message type: {result.get('message_type')}")
                return None
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for response from Neohub")
            return None
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"Websocket closed by other end: {e}")
            return None

    except Exception as e:
        logger.error(f"Error sending command to Neohub {neohub_name}: {e}")
        return None
    finally:
        if websocket:
            try:
                await websocket.close()
                logger.info("WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")

async def check_neohub_compatibility(config: Dict[str, Any], neohub_name: str) -> bool:
    """
    Checks if the Neohub is compatible with the required schedule format (7-day, 6 events).
    Returns True if compatible, False otherwise. Uses neohubapi and send_command for logging.
    """
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(
            f"check_neohub_compatibility: Checking compatibility for {neohub_name}"
        )

    # Ensure the Neohub is connected
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"check_neohub_compatibility: config['neohubs'] = {config['neohubs']}")
    neohub_config = config["neohubs"].get(neohub_name)
    if not neohub_config:
        logging.error(f"Configuration for Neohub {neohub_name} not found.")
        return False

    if neohub_name not in hubs:
        logging.info(f"Connecting to Neohub {neohub_name}...")
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub {neohub_name}.")
            return False

    # Get system data to check ALT_TIMER_FORMAT and HEATING_LEVELS
    try:
        system_data = await send_command(neohub_name, {"GET_SYSTEM": 0})
        if system_data is None:
            logging.error(f"Failed to retrieve system data from Neohub {neohub_name}.")
            return False
    except Exception as e:
        logging.error(f"Error getting system data from Neohub {neohub_name}: {e}")
        return False

    # Check if ALT_TIMER_FORMAT is 4 (7-day mode)
    if not hasattr(system_data, 'ALT_TIMER_FORMAT') or system_data.ALT_TIMER_FORMAT != 4:
        logging.error(f"Neohub {neohub_name} is not configured for a 7-day schedule.")
        return False
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"check_neohub_compatibility: Neohub {neohub_name} ALT_TIMER_FORMAT is configured for 7-day schedule.")

    # Check if HEATING_LEVELS is 6 (6 comfort levels)
    if not hasattr(system_data, 'HEATING_LEVELS') or system_data.HEATING_LEVELS != 6:
        logging.error(f"Neohub {neohub_name} does not support 6 comfort levels.")
        return False

    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"check_neohub_compatibility: Neohub {neohub_name} supports 6 comfort levels.")

    logging.info(f"Neohub {neohub_name} is compatible")
    return True

async def update_heating_schedule():
    """
    Orchestrates the heating update process:
    1. Fetches and consolidates raw bookings by (Hub, Zone).
    2. Calculates the final heating schedule for each consolidated zone, applying preheat/temps.
    3. Stores/Updates the NeoHub profile using the profile ID if it exists.
    4. Activates the correct profile on the relevant zone device.
    """
    logging.info("Starting update_heating_schedule...")

    # 1. Fetch and Consolidate Raw Data
    # Returns: { (neohub_name, zone_name): [raw_resource_dict1, raw_resource_dict2, ...] }
    consolidated_bookings_map = get_bookings_and_locations()
    external_temp = await get_external_temperature()
    
    if not consolidated_bookings_map:
        logging.warning("No bookings found or configuration/fetch failed. Skipping update.")
        return

    # 2. Process, Store, and Activate each unique profile
    
    for (neohub_name, zone_name), raw_bookings in consolidated_bookings_map.items():
        
        profile_name = f"{zone_name} Schedule" # The new stateful profile name
        
        logging.info(f"Processing schedule for Zone: '{zone_name}' on Hub: '{neohub_name}'")

        # a) Calculate the final schedule (This is where the complex logic will go next)
        # Note: We pass raw_bookings, not individual ones.
        final_schedule_data = await calculate_schedule(
            neohub_name, 
            zone_name, 
            raw_bookings, 
            external_temp
        )
        
        if not final_schedule_data:
            logging.warning(f"Calculate schedule returned empty data for {zone_name}. Skipping profile store.")
            continue

        # b) Store/Update the profile (Uses GET_PROFILE to handle create/update via ID)
        # We need to assume store_profile2 is patched to handle profile_name and profile_id
        profile_id = await store_profile2(neohub_name, profile_name, final_schedule_data)
        
        if profile_id is None:
            logging.error(f"Failed to create or update profile for zone {zone_name}. Cannot activate.")
            continue

        # c) Activate the profile
        # We assume activate_profile_on_zone finds the correct NeoStat device and applies the ID.
        await activate_profile_on_zone(neohub_name, zone_name, profile_id)

    logging.info("Finished update_heating_schedule.")

def main():
    """Main application function."""
    # Use the LOGGING_LEVEL environment variable
    logging_level = getattr(logging, LOGGING_LEVEL.upper(), logging.INFO)
    logging.basicConfig(level=logging_level)

    parser = argparse.ArgumentParser(
        description="Run the ChurchSuite Heatmiser Integration App"
    )
    parser.add_argument(
        "--config",
        default=CONFIG_FILE,
        help="Path to the configuration file (default: config/config.json)",
    )
    args = parser.parse_args()
    config_file = args.config
    global config
    config = load_config(config_file)
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    for neohub_name, neohub_config in config["neohubs"].items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()
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
        while True:
            time.sleep(600)
    except KeyboardInterrupt:
        logging.info("Shutting down scheduler...")
        scheduler.shutdown()
        logging.info("Closing Neohub connections...")
#       close_connections()
        logging.info("Exiting...")

if __name__ == "__main__":
    main()
