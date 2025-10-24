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
from typing import Dict, Any, List, Optional, Union, Tuple
import websockets # type: ignore
import ssl
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError, WebSocketClient # type: ignore

_command_id_counter = itertools.count(start=100)

# Configuration
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
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()  # Get logging level from env
# Set up logging
numeric_level = getattr(logging, LOGGING_LEVEL, logging.INFO)
logging.basicConfig(
    level=numeric_level,
    format='%(levelname)s:%(name)s:%(message)s'
)
# Suppress noisy logs from websockets
logging.getLogger("websockets").setLevel(logging.INFO)

# Type definitions
ScheduleSegment = Union[float, str] # Can be a temperature (float) or a command (str like 'sleep')
ScheduleEntry = Dict[str, Union[datetime.time, ScheduleSegment]]

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

def validate_config(config: Dict[str, Any]) -> bool:
    if "neohubs" not in config or not config["neohubs"]:
        logging.error("No Neohubs found in configuration.")
        return False
    if "locations" not in config or not config["locations"]:
        logging.error("No locations found in configuration.")
        return False
    return True

def _get_location_config(location_name: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Safely retrieves the configuration for a specific location from the 'locations' 
    section of the main config.

    Args:
        location_name: The name of the location (e.g., 'Main Church - Chancel').
        config: The main application configuration dictionary.
        
    Returns:
        The location configuration dictionary, or None if not found.
    """
    try:
        return config["locations"][location_name]
    except KeyError:
        # NOTE: Do not raise an error here; log a warning and return None to allow default behavior.
        logging.warning(f"Configuration not found for location: {location_name}. Using defaults.")
        return None

def build_neohub_zone_map(config: Dict[str, Any]) -> Dict[str, Tuple[str, str]]:
    """
    Builds a comprehensive, flattened map from NeoHub Zone Name to its 
    controlling NeoHub and its associated ChurchSuite Location(s).
    
    Returns:
        A dictionary mapping:
        Zone Name (str) -> (NeoHub Name (str), Parent Location Name (str))
        Note: If a zone is controlled by multiple locations, it maps to the first one found.
    """
    zone_map: Dict[str, Tuple[str, str]] = {}
    
    locations_config = config.get("locations", {})
    
    for location_name, loc_config in locations_config.items():
        neohub_name = loc_config.get("neohub")
        zone_names = loc_config.get("zones", [])
        
        if not neohub_name or not zone_names:
            logging.warning(f"Config for location '{location_name}' missing neohub or zones. Skipping map generation for this entry.")
            continue
            
        for zone_name in zone_names:
            if zone_name not in zone_map:
                # The map key is the zone name (from the aggregated schedule)
                # The value is the NeoHub name and the ChurchSuite Location name
                zone_map[zone_name] = (neohub_name, location_name)
            else:
                # This handles zones shared by multiple ChurchSuite locations
                existing_neohub, existing_location = zone_map[zone_name]
                logging.debug(
                    f"Zone '{zone_name}' is shared. Already mapped to NeoHub '{existing_neohub}' (Location: '{existing_location}'). "
                    f"Ignoring redundant mapping from '{location_name}'."
                )

    logging.info(f"Configuration map built for {len(zone_map)} unique NeoHub Zones.")
    return zone_map

def _calculate_location_preheat_minutes(
    location_name: str, 
    current_external_temp: Optional[float], 
    config: Dict[str, Any]
) -> int:
    """
    Calculates the required pre-heat time in minutes for a single ChurchSuite location,
    dynamically adjusting based on external temperature and location-specific config.
    
    This calculates the *requirement* that a heating zone must meet.

    Args:
        location_name: The name of the ChurchSuite location.
        current_external_temp: The current external temperature in Celsius, or None if unavailable.
        config: The main application configuration dictionary.

    Returns:
        The dynamically calculated pre-heat time in minutes (integer) for this location.
    """
    base_preheat = PREHEAT_TIME_MINUTES
    
    location_config = _get_location_config(location_name, config)
    if not location_config:
        return base_preheat

    # Get specific configuration values for this location, using defaults if keys are missing
    heat_loss_factor = location_config.get("heat_loss_factor", 1.0)
    min_external_temp = location_config.get("min_external_temp", 5) 

    if current_external_temp is None:
        logging.warning(f"External temperature is unknown. Using base preheat time ({base_preheat} min).")
        return base_preheat

    # Calculate how far below the location's minimum external temperature the current temp is.
    # The max(0.0, ...) ensures preheat adjustment is only added when it's colder than the threshold.
    temp_difference = max(0.0, min_external_temp - current_external_temp)

    # Calculate adjustment: temp_difference * adjustment_per_degree * heat_loss_factor
    preheat_adjustment = temp_difference * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE * heat_loss_factor

    total_preheat_minutes = int(round(base_preheat + preheat_adjustment))

    logging.debug(
        f"Pre-heat for {location_name}: ExtTemp={current_external_temp:.1f}C, "
        f"Diff={temp_difference:.1f}C, Total Preheat: {total_preheat_minutes} min."
    )
    
    # Ensure preheat time is non-negative.
    return max(0, total_preheat_minutes)

def create_aggregated_schedule(
    bookings: List[Dict[str, Any]], 
    current_external_temp: Optional[float], 
    config: Dict[str, Any]
) -> Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]]:
    """
    Processes ChurchSuite bookings to create a single, aggregated heating schedule 
    for each NeoHub Zone.
    """
    
    # Final schedule structure: { zone_name: { day_of_week: [setpoints] } }
    zone_schedule: Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]] = {}
    
    logging.info(f"AGGREGATION START: Processing {len(bookings)} bookings.")
    
    # Initialize schedule with default ECO setpoints for all zones/days
    for location_config in config.get("locations", {}).values():
        for zone_name in location_config.get("zones", []):
            if zone_name not in zone_schedule:
                # Initialize schedule structure for the zone/day
                zone_schedule[zone_name] = {i: [] for i in range(7)}
                # CRITICAL: Add default ECO profile for a full week (00:00 @ ECO_TEMPERATURE)
                for day in range(7):
                    # This ensures the thermostat defaults to ECO if no booking is present
                    zone_schedule[zone_name][day].append({"time": "00:00", "temp": ECO_TEMPERATURE})
                    
    logging.debug(f"AGGREGATION INIT: Initialized zones: {list(zone_schedule.keys())}")
    
    if not bookings:
        logging.info("AGGREGATION END: Bookings list was empty. Returning ECO default schedule.")
        # If no bookings, the initialized ECO schedule is returned
        return zone_schedule

    for booking in bookings:
        location_name = booking.get("resource")
        booking_id = booking.get("id", "N/A")
        
        logging.debug(f"AGGREGATION LOOP: Processing booking ID {booking_id} for location '{location_name}'")
        
        if not location_name:
            logging.warning(f"Skipping booking ID {booking_id}: Missing 'resource' key.")
            continue
        
        # 1. Get location config
        location_config = _get_location_config(location_name, config)
        if not location_config:
            logging.warning(f"Skipping booking ID {booking_id}: No config found for location '{location_name}'.")
            continue

        # 2. Calculate the specific preheat required for THIS location/booking
        required_preheat_minutes = _calculate_location_preheat_minutes(
            location_name, 
            current_external_temp, 
            config
        )
        logging.debug(f"PREHEAT: Location '{location_name}' requires {required_preheat_minutes} minutes of preheat.")

        # 3. Determine all zones associated with this location
        zone_names = location_config.get("zones", [])
        if not zone_names:
            logging.warning(f"Location '{location_name}' has no 'zones' configured. Skipping.")
            continue
        logging.debug(f"ZONES: Location '{location_name}' affects zones: {zone_names}")
            
        try:
            # CRITICAL CHECK: Ensure your booking data contains these keys.
            # If the raw ChurchSuite bookings only contain 'starts_at' and 'ends_at',
            # you need to change the keys below.
            start_time_key = "starts_at" if "starts_at" in booking else "start_time_utc"
            end_time_key = "ends_at" if "ends_at" in booking else "end_time_utc"
            
            start_dt_utc = dateutil.parser.parse(booking[start_time_key])
            end_dt_utc = dateutil.parser.parse(booking[end_time_key])
            
            logging.debug(f"TIME PARSE: UTC Start: {start_dt_utc.isoformat()}, UTC End: {end_dt_utc.isoformat()}")
            
            # Assuming 'TIMEZONE' is an environment variable or global constant
            local_tz = pytz.timezone(TIMEZONE) 
            start_dt_local = start_dt_utc.astimezone(local_tz)
            end_dt_local = end_dt_utc.astimezone(local_tz)

            day_of_week = start_dt_local.weekday() # Monday=0, Sunday=6
            target_temp = location_config.get("default_temp", DEFAULT_TEMPERATURE)
            
            logging.debug(f"TIME CONVERTED: Local Day: {day_of_week}, Local Start: {start_dt_local.strftime('%H:%M')}, Target Temp: {target_temp}")

            # --- Iterate through all zones linked to this location ---
            for zone_name in zone_names:
                
                # Calculate the preheat start time for THIS event
                preheat_start_dt_local = start_dt_local - datetime.timedelta(minutes=required_preheat_minutes)
                
                # Format times as "HH:MM"
                preheat_time_str = preheat_start_dt_local.strftime("%H:%M")
                end_time_str = end_dt_local.strftime("%H:%M")
                
                # Add the pre-heat setpoint (Set to target temp at preheat start time)
                zone_schedule[zone_name][day_of_week].append({
                    "time": preheat_time_str, 
                    "temp": target_temp
                })
                
                # Add the eco setpoint (Set to ECO temp at event end time)
                zone_schedule[zone_name][day_of_week].append({
                    "time": end_time_str, 
                    "temp": ECO_TEMPERATURE
                })
                
                logging.debug(f"SETPOINTS ADDED: Zone '{zone_name}' (Day {day_of_week}): Preheat at {preheat_time_str} ({target_temp}°C), ECO at {end_time_str} ({ECO_TEMPERATURE}°C)")

        except (KeyError, ValueError, TypeError) as e:
            # Note: This is where a KeyError on the time strings would be caught.
            logging.error(f"Error processing booking for {location_name} (ID {booking_id}): {e}. Check time keys in booking data.", exc_info=True)
            continue
            
    # --- Post-processing: Sort and Merge ---
    logging.info("POST-PROCESSING START: Sorting and merging conflicting setpoints.")
    
    for zone, daily_schedule in zone_schedule.items():
        for day, setpoints in daily_schedule.items():
            
            if not setpoints:
                continue

            # 1. Sort setpoints by time
            setpoints.sort(key=lambda x: x["time"])

            # 2. Filter out duplicate times, prioritizing the **highest** temperature.
            unique_setpoints = {} # { "HH:MM": max_temp }

            for sp in setpoints:
                time_str = sp["time"]
                temp = sp["temp"]
                
                if time_str not in unique_setpoints or temp > unique_setpoints[time_str]:
                    unique_setpoints[time_str] = temp
                    logging.debug(f"MERGE: Zone '{zone}' Day {day} @ {time_str}: Set to {temp}°C (Highest temperature chosen).")

            # Convert back to list of dicts and sort again
            final_setpoints = [{"time": t, "temp": temp} for t, temp in unique_setpoints.items()]
            final_setpoints.sort(key=lambda x: x["time"]) 
            
            zone_schedule[zone][day] = final_setpoints

            if final_setpoints:
                # Log the final, merged schedule for visibility
                logging.debug(f"AGGREGATED SCHEDULE: Zone '{zone}' Day {day}: {final_setpoints}")
    
    logging.info(f"AGGREGATION END: Successfully generated schedules for {len(zone_schedule)} NeoHub zones.")
    return zone_schedule

def _format_setpoints_for_neohub(setpoints: List[Dict[str, Union[str, float]]]) -> Dict[str, List[Union[str, float, int, bool]]]:
    """
    Converts a list of setpoint dicts (time, temp) into the Neohub's required
    profile dictionary format (e.g., {'wake': [time, temp, sensitivity, true]}).
    """
    formatted_schedule = {}
    
    # NeoHub uses specific keys for schedule steps (up to 6 setpoints/day)
    step_keys = ["wake", "level1", "level2", "level3", "level4", "level5", "sleep"]
    
    # Ensure we don't exceed the number of supported steps
    safe_setpoints = setpoints[:len(step_keys)]

    for i, sp in enumerate(safe_setpoints):
        key = step_keys[i]
        time_str = sp["time"]
        temp_float = sp["temp"]
        
        # The NeoHub schedule format requires four items: 
        # [time, temperature, sensitivity, enabled (true)]
        formatted_schedule[key] = [
            time_str, 
            temp_float, 
            TEMPERATURE_SENSITIVITY, # Uses the global config variable
            True # Always enabled
        ]
        
    return formatted_schedule

# REPLACE THE OLD apply_single_zone_profile with this updated version
async def apply_single_zone_profile(
    neohub_name: str, 
    zone_name: str, 
    daily_schedule: Dict[int, List[Dict[str, Union[str, float]]]],
    profile_prefix: str # ADDED THIS PARAMETER
) -> None:
    """
    Applies the full 7-day schedule to a single zone on a specific Neohub.
    This function implements the fix for days with no bookings.
    """
    DAY_MAP = {
        0: "monday", 1: "tuesday", 2: "wednesday", 3: "thursday",
        4: "friday", 5: "saturday", 6: "sunday"
    }
    
    # The final payload structure for NeoHub's STORE_PROFILE2 command
    neohub_profile_data: Dict[str, Dict[str, List[Union[str, float, int, bool]]]] = {}

    for day_of_week in range(7):
        # ... (Rest of the logic remains the same, including the ECO setpoint fix) ...
        # ... (We will skip the full block here for brevity, assume the inner logic is the same) ...
        day_key = DAY_MAP[day_of_week]
        setpoints = daily_schedule.get(day_of_week, [])
        
        if not setpoints:
            default_eco_setpoint = [{"time": "00:00", "temp": ECO_TEMPERATURE}]
            logging.info(f"Day {day_key} for zone {zone_name} has no bookings. Applying 00:00 ECO schedule.")
            setpoints_to_format = default_eco_setpoint
        else:
            setpoints_to_format = setpoints
        
        neohub_profile_data[day_key] = _format_setpoints_for_neohub(setpoints_to_format)

    # 1. POST (Store) the calculated profile
    profile_name = f"{profile_prefix}: {zone_name}" 
    await store_profile2(neohub_name, profile_name, neohub_profile_data)
    
    # 2. RUN (Activate) the profile IF it is the Current Week
    if profile_prefix == "Current Week":
        if neohub_name in hubs:
            hub = hubs[neohub_name]
            command = {"RUN_PROFILE": profile_name}
            
            # CRITICAL FIX: Use the hub's method (e.g., send_command) to execute the NeoHub command
            try:
                # Assuming the NeoHub object has a send_command method or similar
                await hub.send_command(command) 
                logging.info(f"Successfully sent RUN_PROFILE command to Neohub {neohub_name} for profile '{profile_name}'.")
            except Exception as e:
                logging.error(f"Failed to send RUN_PROFILE command to {neohub_name}: {e}")
        else:
            logging.error(f"Cannot run profile: Neohub '{neohub_name}' not found in connected hubs.")

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
        for key in ["STORE_PROFILE", "STORE_PROFILE2"]:
            if key in command and isinstance(command[key], dict):
                is_profile_command = True
                break
            
    if is_profile_command:
        # Bypass the broken hub._send() for profile commands
        return await _send_raw_profile_command(hub, command)
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

async def store_profile2(neohub_name: str, profile_name: str, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Stores a heating profile on the Neohub, passing a Python dictionary structure directly to avoid double-encoding."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")

    # 1. CREATE THE COMMAND PAYLOAD (clean Python dict with float temps and P_TYPE)
    # This structure is exactly what the Hub expects under the main COMMAND key.
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
    # The neohubapi library's _send() will now correctly serialize this dictionary 
    # for the WebSocket transport, avoiding the double-encoding issue.
    response = await send_command(neohub_name, inner_payload)
    return response

async def _send_raw_profile_command(hub: NeoHub, command: Dict[str, Any]) -> Optional[Any]:
    """
    Manually constructs, sends, and waits for the response for the STORE_PROFILE2 
    command. It uses manual string injection, single-quote replacement, and final 
    backslash stripping to satisfy the finicky Neohub parser while preserving the 
    required four-item schedule list with unquoted booleans.
    """
    global _command_id_counter
    
    hub_token = getattr(hub, '_token', None)
    hub_client = getattr(hub, '_client', None)
    
    if not hub_token or not hub_client:
        logging.error("Could not access private token or client (_token or _client) for raw send.")
        return None

    try:  # <--- START OF CORRECTED TRY BLOCK
        # 0. Preparation: The input 'command' now relies on Python True/False 
        # being correctly serialized to unquoted true/false by json.dumps.
        command_to_send = command
        
        # 1. Serialize the command to double-quoted JSON (this results in unquoted true/false)
        command_id = next(_command_id_counter)
        command_value_str = json.dumps(command_to_send, separators=(',', ':'))

        # 2. **HACK 1: Convert all double quotes to single quotes** for the inner command content.
        # This is CRUCIAL: it leaves the unquoted 'true'/'false' untouched.
        command_value_str_hacked = command_value_str.replace('"', "'")
        
        # 3. **HACK 2: Manually construct the INNER_MESSAGE string**
        message_str = (
            '{\\"token\\": \\"' + hub_token + '\\", '
            '\\"COMMANDS\\": ['
                '{\\"COMMAND\\": \\"' + command_value_str_hacked + '\\", ' # Inject the single-quoted string
                '\\"COMMANDID\\": ' + str(command_id) + '}' # COMMANDID is not quoted
            ']}'
        )

        # 4. Construct the final payload dictionary (outer wrapper)
        final_payload_dict = {
            "message_type": "hm_get_command_queue",
            "message": message_str 
        }
        
        # 5. **Final Serialization & Escaping Hacks**
        final_payload_string = json.dumps(final_payload_dict) 
        
        # **HACK 3 (User Request): Strip excess escaping**
        final_payload_string = final_payload_string.replace('\\\\\\"', '\\"')
        
        # 6. Hook into the response mechanism
        raw_connection = getattr(hub_client, '_websocket', None)
        raw_ws_send = getattr(raw_connection, 'send', None) if raw_connection else None
        pending_requests = getattr(hub_client, '_pending_requests', None)
        request_timeout = getattr(hub_client, '_request_timeout', 60) 
        
        if not raw_ws_send or pending_requests is None:
             raise AttributeError("Could not find internal mechanisms needed for raw send/receive.")
        
        future: asyncio.Future[Any] = asyncio.Future()
        pending_requests[command_id] = future

        logging.debug(f"Raw Sending: {final_payload_string}")
        
        # 7. Send and wait
        await raw_ws_send(final_payload_string)
        response_dict = await asyncio.wait_for(future, timeout=request_timeout)
        
        # 8. Process the response (Using robust logic to prevent crash)
        logging.debug(f"Received STORE_PROFILE2 response (COMMANDID {command_id}): {response_dict}")

        profile_data = response_dict.get("STORE_PROFILE2")

        if profile_data and isinstance(profile_data, dict) and "PROFILE_ID" in profile_data:
             profile_id = profile_data["PROFILE_ID"]
             logging.info(f"Successfully stored profile with ID: {profile_id}")
             return {"command_id": command_id, "status": "Success", "profile_id": profile_id}
        elif isinstance(response_dict, dict) and "error" in response_dict:
             logging.error(f"Neohub returned error for command {command_id}: {response_dict['error']}")
             return {"command_id": command_id, "status": "Error", "neohub_error": response_dict['error']}
        else:
             logging.error(f"Neohub returned unexpected response for command {command_id}: {response_dict}. Check app/device for submission status.")
             return {"command_id": command_id, "status": "Unexpected Response", "response": response_dict}

    except asyncio.TimeoutError: # <--- REQUIRED EXCEPT CLAUSE
        logging.error(f"Timeout waiting for response for command {command_id}.")
        return {"command_id": command_id, "status": "Timeout"}
    except Exception as e: # <--- REQUIRED EXCEPT CLAUSE
        logging.error(f"Error during raw WebSocket send/receive for profile command: {e}")
        return None
    finally: # <--- REQUIRED FINALLY CLAUSE
        # Clean up the pending request
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

async def log_existing_profile(neohub_name: str, profile_name: str) -> None:
    """
    Fetches and logs the current settings of a specific profile on a NeoHub for debugging.
    This runs only if LOGGING_LEVEL is set to DEBUG.
    """
    global LOGGING_LEVEL
    if LOGGING_LEVEL != "DEBUG":
        return 

    logging.debug(f"Attempting to fetch existing profile '{profile_name}' on Neohub {neohub_name} for comparison...")
    
    # Use GET_PROFILE to retrieve the schedule data for the named profile (validated command)
    command = {"GET_PROFILE": profile_name}
    response = await send_command(neohub_name, command)
    
    # BUG FIX: Use attribute access (response.status) or safe attribute access (getattr/hasattr)
    # Check if response exists, if its status attribute is 'success', and if it has a data attribute
    if response and getattr(response, "status", None) == "success" and hasattr(response, "data"):
        # Log the received data cleanly. response.data is assumed to be a dictionary or list.
        logging.debug(
            f"Existing Profile Data for '{profile_name}' on {neohub_name}:\n{json.dumps(response.data, indent=4)}"
        )
    else:
        # Safely log the status if available, otherwise 'N/A'
        status = getattr(response, "status", "N/A")
        logging.debug(
            f"Failed to fetch profile '{profile_name}' on {neohub_name}. Response Status: {status}"
        )

async def apply_schedule_to_heating(
    neohub_name: str, profile_name: str, schedule_data: Dict[str, Any]
) -> None:
    """Applies the heating schedule to the Heatmiser system by storing the profile."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(
            f"apply_schedule_to_heating: neohub_name={neohub_name}, profile_name={profile_name}, schedule_data={schedule_data}"
        )
    # Log the existing profile for comparison
    await log_existing_profile(neohub_name, profile_name)
    # Store the profile using the neohubapi library's store_profile2 function
    response = await store_profile2(neohub_name, profile_name, schedule_data)

    if response:
         logging.info(
             f"Successfully stored profile {profile_name} on Neohub {neohub_name}"
         )
    else:
         logging.error(f"Failed to store profile {profile_name} on Neohub {neohub_name}")
    
    # Check if the profile was stored successfully
    # try:
    #    stored_profile = await get_profile(neohub_name, "Test")
    #    if stored_profile:
    #        logging.info(f"Successfully stored profile 'Test' on Neohub {neohub_name}")
    #    else:
    #        logging.error(f"Failed to store profile 'Test' on Neohub {neohub_name}")
    # except Exception as e:
    #    logging.error(f"Error retrieving profile 'Test' from Neohub {neohub_name}: {e}")

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

async def apply_aggregated_schedules(
    aggregated_schedules: Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]], 
    profile_prefix: str, 
    config: Dict[str, Any],
    neohub_zone_map: Dict[str, Tuple[str, str]] # Accepts the new map
) -> None:
    """
    Orchestrates the application of the aggregated schedules to the correct Neohubs/Zones 
    using the pre-built NeoHub/Zone map.
    """
    
    tasks = []
    
    # Loop over the aggregated schedules. The key is the NeoHub Zone Name (e.g., 'Main Church')
    for zone_name, daily_schedule in aggregated_schedules.items(): 
        
        # CRITICAL FIX: Look up the configuration using the Zone Name key against the map.
        map_entry = neohub_zone_map.get(zone_name)
        
        if not map_entry:
            logging.error(
                f"Schedule found for Zone '{zone_name}', but no matching NeoHub configuration was found in the map. "
                f"The schedule aggregation step may have produced an invalid zone name. Skipping."
            )
            continue
            
        # Unpack the required NeoHub Name and the associated Location Name (for logging/reference)
        neohub_name, parent_location_name = map_entry 
        
        if not neohub_name:
            logging.error(f"Configuration for Zone '{zone_name}' is incomplete (missing neohub in map). Skipping.")
            continue

        # Check if the NeoHub is actually connected (assuming 'hubs' is a global dict of connected hubs)
        # NOTE: You must ensure 'hubs' is accessible in this function's scope.
        if neohub_name in hubs: 
            logging.info(f"Adding task to apply schedule for Zone '{zone_name}' (Parent Loc: '{parent_location_name}') on Hub '{neohub_name}'.")
            
            # Add a task to apply the schedule to the specific physical zone
            tasks.append(
                apply_single_zone_profile(neohub_name, zone_name, daily_schedule, profile_prefix) 
            )
        else:
            logging.error(f"Cannot apply schedule for zone '{zone_name}': Neohub '{neohub_name}' not in connected hubs. Connected hubs: {list(hubs.keys())}. Did connection fail?")

    if tasks:
        logging.info(f"Applying {len(tasks)} zone profiles for {profile_prefix}.")
        await asyncio.gather(*tasks)
    else:
        # This warning is now correctly issued only if the aggregated_schedules was empty OR no zone in it was configured.
        logging.warning(f"No profiles generated or applied for {profile_prefix}.")

# --- MODIFIED FUNCTION ---
def calculate_schedule(
    booking: Dict[str, Any], config: Dict[str, Any], external_temperature: Optional[float], resource_map: Dict[int, str]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the heating schedule for a single booking and returns it
    with metadata for aggregation.
    """
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
    
    # Use global constants
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
    for day in days:
        profile_data[day] = {}

    def add_level(
        day_data: Dict[str, Any],
        level_name: str,
        event_time: datetime.datetime,
        temperature: float,
    ):
        """Adds a level to the day's schedule."""
        # Note: The NeoHub API expects a list of [time_str, temp, step_duration_min, is_active]
        day_data[level_name] = [
            event_time.strftime("%H:%M"),
            float(temperature),  # Ensure temperature is a float
            5,  # Set to 5 (or the desired step duration)
            True,  # Set to True (active)
        ]

    # Assume all bookings are for the current day for simplicity in the loop structure,
    # but the time calculation is correct. The aggregation logic handles overlapping days.
    # Note: This logic assumes ChurchSuite provides separate bookings for midnight crossing.
    for day in days: # This loop structure is preserved from the original user's code.
        day_schedule = profile_data[day]
        
        # Start levels (Preheat start time)
        add_level(day_schedule, "wake", start_time - preheat_time, DEFAULT_TEMPERATURE)
        add_level(day_schedule, "level2", start_time - preheat_time, DEFAULT_TEMPERATURE)
        
        # End levels (Booking end time - switch to ECO)
        add_level(day_schedule, "level3", end_time, ECO_TEMPERATURE)
        add_level(day_schedule, "level4", end_time, ECO_TEMPERATURE)
        add_level(day_schedule, "sleep", end_time, ECO_TEMPERATURE)
        add_level(day_schedule, "level1", end_time, ECO_TEMPERATURE)
    
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"calculate_schedule: Calculated schedule: {profile_data}")
        
    # --- MODIFIED RETURN VALUE ---
    return {
        "location_name": location_name,
        "neohub": neohub_name,
        "zones": zones,
        "profile_data": profile_data,
    }

# --- MODIFIED FUNCTION (Using the collected schedules for aggregation and application) ---
async def update_heating_schedule() -> None:
    """
    Updates the heating schedule based on upcoming bookings,
    aggregating schedules by neohub and zone.
    """
    logging.info("--- STARTING HEATING SCHEDULE UPDATE PROCESS ---")
    global config
    
    # 1. Configuration Validation
    if config is None:
        logging.error("Configuration not loaded. Exiting.")
        return
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        return
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    # NEW: Build the centralized configuration map (Place this after config validation)
    neohub_zone_map = build_neohub_zone_map(config)
    
    # 2. Timezone and Week Calculation
    location_timezone_name = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
    try:
        location_timezone = pytz.timezone(location_timezone_name)
        logging.debug(f"Using timezone: {location_timezone_name}")
    except pytz.exceptions.UnknownTimeZoneError:
        logging.error(
            f"Timezone '{location_timezone_name}' is invalid. Defaulting to Europe/London. "
            "Please set the CHURCHSUITE_TIMEZONE environment variable with a valid timezone name (e.g., 'Europe/London')."
        )
        location_timezone = pytz.timezone("Europe/London")

    today = datetime.datetime.now(location_timezone).replace(tzinfo=None)
    current_week_start = today - datetime.timedelta(days=today.weekday())
    current_week_end = current_week_start + datetime.timedelta(days=6)
    next_week_start = current_week_end + datetime.timedelta(days=1)
    next_week_end = next_week_start + datetime.timedelta(days=6)
    
    logging.info(f"Current Date: {today.date()}")
    logging.info(f"Current Week Range: {current_week_start.date()} to {current_week_end.date()}")
    logging.info(f"Next Week Range: {next_week_start.date()} to {next_week_end.date()}")

    # 3. Fetch Bookings and Resources
    data = get_bookings_and_locations()
    if data:
        booked_resources = data.get("booked_resources", [])
        resources = data.get("resources", [])
        
        logging.info(f"Fetched {len(booked_resources)} total bookings and {len(resources)} resources from ChurchSuite.")

        if not booked_resources:
            logging.info("No bookings to process. Exiting schedule update early.")
            return

        if not resources:
            logging.error("No resources found. Cannot map bookings to locations. Exiting schedule update early.")
            return

        resource_map = {r["id"]: r["name"] for r in resources}
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"update_heating_schedule: resource_map = {resource_map}")

        # 4. Filter Bookings by Week
        current_week_bookings = []
        next_week_bookings = []

        for booking in booked_resources:
            start_time_str = booking.get("starts_at")
            if start_time_str:
                try:
                    parsed_dt = dateutil.parser.parse(start_time_str)

                    if parsed_dt.tzinfo is None or parsed_dt.utcoffset() is None:
                        utc_dt = parsed_dt.replace(tzinfo=pytz.utc)
                        logging.warning(f"Booking time for ID {booking.get('id', 'unknown')} was naive, assuming UTC")
                    else:
                        utc_dt = parsed_dt.astimezone(pytz.utc)

                    local_start_dt = utc_dt.astimezone(location_timezone).replace(tzinfo=None)

                    if current_week_start <= local_start_dt <= current_week_end:
                        current_week_bookings.append(booking)
                    elif next_week_start <= local_start_dt <= next_week_end:
                        next_week_bookings.append(booking)
                except dateutil.parser.ParserError as e:
                    logging.error(f"Failed to parse datetime for booking ID {booking.get('id', 'unknown')}: {e}")
            else:
                logging.warning(f"Booking with id {booking.get('id', 'unknown')} has no 'starts_at' time.")

        logging.info(f"Filtered Bookings: Current Week: {len(current_week_bookings)}, Next Week: {len(next_week_bookings)}")
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"Current Week Bookings: {current_week_bookings}")


        # 5. Calculate Individual Schedules and Get External Temperature
        current_week_schedules: List[Dict[str, Any]] = []
        next_week_schedules: List[Dict[str, Any]] = []
        
        external_temperature = get_external_temperature()
        logging.info(f"Fetched external temperature: {external_temperature}°C")

        # The loop below is **redundant** for final schedule application 
        # (as the raw bookings are used later), but essential for logging if 'calculate_schedule' works.
        for location_name, location_config in config["locations"].items():
            neohub_name = location_config["neohub"]
            resource_ids = [resource_id for resource_id, name in resource_map.items() if name == location_name]
            
            # Process current week bookings
            for booked_resource in current_week_bookings:
                if booked_resource["resource_id"] in resource_ids:
                    logging.debug(f"PROCESSING: Current Week Booking for Location: {location_name}")
                    
                    schedule_data = calculate_schedule(booked_resource, config, external_temperature, resource_map)
                    
                    if schedule_data:
                        current_week_schedules.append(schedule_data) 
                        logging.debug(f"SCHEDULE_CALCULATED: Current Week for {location_name}. Schedule length: {len(schedule_data.get(location_name, {}))}")
                    else:
                        logging.warning(f"CALCULATE_SCHEDULE returned None/Empty for {location_name} (Current Week).")

            # Process next week bookings
            for booked_resource in next_week_bookings:
                if booked_resource["resource_id"] in resource_ids:
                    logging.debug(f"PROCESSING: Next Week Booking for Location: {location_name}")

                    schedule_data = calculate_schedule(booked_resource, config, external_temperature, resource_map)
                    
                    if schedule_data:
                        next_week_schedules.append(schedule_data)
                        logging.debug(f"SCHEDULE_CALCULATED: Next Week for {location_name}. Schedule length: {len(schedule_data.get(location_name, {}))}")
                    else:
                        logging.warning(f"CALCULATE_SCHEDULE returned None/Empty for {location_name} (Next Week).")
        
        logging.info(f"Intermediate Schedules Calculated (Current Week): {len(current_week_schedules)}")
        logging.info(f"Intermediate Schedules Calculated (Next Week): {len(next_week_schedules)}")


        # 6. AGGREGATE SCHEDULES BY ZONE (The Critical Step)
        aggregated_current_schedules = create_aggregated_schedule(
            current_week_bookings, 
            external_temperature, 
            config
        )
        logging.info(f"AGGREGATION RESULT (Current Week): {len(aggregated_current_schedules)} final locations/zones scheduled.")
        logging.debug(f"DEBUG: create_aggregated_schedule returned Current Week: {aggregated_current_schedules}")
        
        aggregated_next_schedules = create_aggregated_schedule(
            next_week_bookings, 
            external_temperature, 
            config
        )
        logging.info(f"AGGREGATION RESULT (Next Week): {len(aggregated_next_schedules)} final locations/zones scheduled.")
        logging.debug(f"DEBUG: create_aggregated_schedule returned Next Week: {aggregated_next_schedules}")

        # 7. APPLY AGGREGATED SCHEDULES
        await apply_aggregated_schedules(
            aggregated_current_schedules, "Current Week", config
        )
        await apply_aggregated_schedules(
            aggregated_next_schedules, "Next Week", config
        )

    else:
        logging.info("No data received from ChurchSuite.")
    
    logging.info("--- HEATING SCHEDULE UPDATE PROCESS COMPLETE ---")

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