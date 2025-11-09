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
from types import SimpleNamespace # We need this import to check the type
from typing import Dict, Any, List, Optional, Union, Tuple
import websockets # type: ignore
import ssl
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError, WebSocketClient # type: ignore

class CommandIdManager:
    """Manages the command ID, resetting at the start of a logical unit (like update_heating_schedule)."""
    
    def __init__(self, start_id: int):
        self.start_id = start_id
        self._counter = itertools.count(start=start_id)

    # The __iter__ method returns the object itself, satisfying the iterator protocol.
    def __iter__(self):
        return self
        
    # The __next__ method is what allows the object to be passed to next().
    # This replaces the old __call__ method.
    def __next__(self) -> int:
        """Returns the next command ID."""
        return next(self._counter)

    def reset(self):
        """Resets the counter for a new loop iteration."""
        self._counter = itertools.count(start=self.start_id)
        logging.info(f"Command ID counter for bulk commands reset to {self.start_id}.")

# Define the global manager instance (starts at 100)
_command_id_counter = CommandIdManager(start_id=100)

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
MIN_FIRMWARE_VERSION = 2079
REQUIRED_HEATING_LEVELS = 6
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
NEOHUB_SLOTS = ["wake", "level1", "level2", "level3", "level4", "sleep"]
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

# Other Constants
RETRY_DELAYS = [0.5, 2.0] # Delay before 2nd and 3rd attempt, respectively
MAX_ATTEMPTS = 1 + len(RETRY_DELAYS) # Total attempts: 1 initial + 2 retries = 3

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

def build_zone_to_neohub_map(config: Dict[str, Any]) -> Dict[str, str]:
    """
    Builds a definitive map from NeoHub Zone Name to its controlling NeoHub Name.
    
    Returns:
        A dictionary mapping:
        NeoHub Zone Name (str) -> NeoHub Name (str)
    """
    zone_to_neohub: Dict[str, str] = {}
    
    locations_config = config.get("locations", {})
    
    for location_name, loc_config in locations_config.items():
        neohub_name = loc_config.get("neohub")
        zone_names = loc_config.get("zones", [])
        
        if not neohub_name or not zone_names:
            logging.warning(f"Config for location '{location_name}' missing neohub or zones. Skipping map generation for this entry.")
            continue
            
        for zone_name in zone_names:
            if zone_name in zone_to_neohub and zone_to_neohub[zone_name] != neohub_name:
                # This is a critical warning: A single zone must not be controlled by two different NeoHubs.
                # If this happens, only the first mapping is kept to prevent ambiguous control.
                logging.error(
                    f"CRITICAL CONFIG ERROR: Zone '{zone_name}' is mapped to two different NeoHubs: "
                    f"'{zone_to_neohub[zone_name]}' (from previous location) and '{neohub_name}' (from '{location_name}'). "
                    f"Keeping the existing mapping."
                )
            elif zone_name not in zone_to_neohub:
                zone_to_neohub[zone_name] = neohub_name

    logging.info(f"Configuration map built for {len(zone_to_neohub)} unique NeoHub Zones.")
    return zone_to_neohub

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
    config: Dict[str, Any],
    resource_id_to_name: Dict[int, str] 
) -> Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]]:
    """
    Processes ChurchSuite bookings to create a single, aggregated heating schedule 
    for each NeoHub Zone, incorporating setpoint optimization and slot limiting.
    """
    
    # Final schedule structure: { zone_name: { day_of_week_index: [setpoints] } }
    zone_schedule: Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]] = {}
    
    logging.info(f"AGGREGATION START: Processing {len(bookings)} bookings.")
    
    # 1. Initialize schedule structure for all zones/days
    for location_name, location_config in config.get("locations", {}).items():
        for zone_name in location_config.get("zones", []):
            if zone_name not in zone_schedule:
                zone_schedule[zone_name] = {i: [] for i in range(7)}
    
    if not bookings:
        logging.info("AGGREGATION END: Bookings list was empty. Returning fully optimized ECO default schedule.")
        # If no bookings, we manually inject the 00:00 ECO to all days/zones for resilience
        for zone, daily_schedule in zone_schedule.items():
            for day in range(7):
                zone_schedule[zone][day].append({"time": "00:00", "temp": ECO_TEMPERATURE})
        return zone_schedule

    first_booking_id = bookings[0].get("id") if bookings else None
    
    for booking in bookings:
        booking_id = booking.get("id", "N/A")
        resource_id = booking.get("resource_id")
        
        # ... [Unchanged pre-processing logic to get location, preheat, and zone names] ...
        location_name = resource_id_to_name.get(resource_id)
        if not location_name:
            logging.warning(f"Skipping booking ID {booking_id}: Resource ID {resource_id} not found in the resource map. Check ChurchSuite data.")
            continue
        
        if location_name not in config["locations"]:
            location_config = None
            for cfg_name, cfg in config["locations"].items():
                 if location_name == cfg_name or location_name in cfg.get("aliases", []):
                    location_config = cfg
                    location_name = cfg_name
                    break
            if not location_config:
                logging.warning(f"Skipping booking ID {booking_id}: No config found for location '{location_name}'.")
                continue
        
        location_config = config["locations"][location_name]

        required_preheat_minutes = _calculate_location_preheat_minutes(location_name, current_external_temp, config)
        zone_names = location_config.get("zones", [])

        try:
            # CRITICAL TIME PARSING
            start_dt_utc = dateutil.parser.parse(booking["starts_at"])
            end_dt_utc = dateutil.parser.parse(booking["ends_at"])
            
            local_tz = pytz.timezone(TIMEZONE) 
            start_dt_local = start_dt_utc.astimezone(local_tz).replace(tzinfo=None)
            end_dt_local = end_dt_utc.astimezone(local_tz).replace(tzinfo=None)

            start_day_of_week = start_dt_local.weekday()
            end_day_of_week = end_dt_local.weekday()
            target_temp = location_config.get("default_temp", DEFAULT_TEMPERATURE)
            
            preheat_start_dt_local = start_dt_local - datetime.timedelta(minutes=required_preheat_minutes)
            preheat_time_str = preheat_start_dt_local.strftime("%H:%M")
            end_time_str = end_dt_local.strftime("%H:%M")

            # --- Add Setpoints (The raw data) ---
            for zone_name in zone_names:
                # Heat ON setpoint
                zone_schedule[zone_name][start_day_of_week].append({
                    "time": preheat_time_str, "temp": target_temp
                })
                
                # ECO OFF setpoint (on the END day)
                zone_schedule[zone_name][end_day_of_week].append({
                    "time": end_time_str, "temp": ECO_TEMPERATURE
                })
                if start_day_of_week != end_day_of_week:
                    logging.debug(f"PROBE G (Cross-Midnight): Booking ID {booking_id} spans midnight. ECO setpoint added to Day {end_day_of_week}.")

        except (KeyError, ValueError, TypeError) as e:
            logging.error(f"Error processing booking for {location_name} (ID {booking_id}): {e}.", exc_info=True)
            continue
            
    # --- Post-processing: Sort, Merge, Optimize, and Limit ---
    logging.info("POST-PROCESSING START: Sorting, merging, optimizing, and limiting setpoints.")
    
    for zone, daily_schedule in zone_schedule.items():
        for day, setpoints in daily_schedule.items():
            
            # 1. Merge and Sort (Unique setpoints, max temp priority)
            unique_setpoints = {} 
            for sp in setpoints:
                time_str = sp["time"]
                temp = sp["temp"]
                if time_str not in unique_setpoints or temp > unique_setpoints[time_str]:
                    unique_setpoints[time_str] = temp

            working_setpoints = [
                {"time": t, "temp": temp} 
                for t, temp in unique_setpoints.items()
            ]
            working_setpoints.sort(key=lambda x: tuple(map(int, x["time"].split(':'))))

            # --- 2. Cross-Midnight 00:00 ECO Removal (CRITICAL FIX) ---
            # If the profile has events and the first point is 00:00 ECO, remove it.
            # This allows previous day's comfort setting to bleed past midnight.
            if len(working_setpoints) > 1 and \
               working_setpoints[0]["time"] == "00:00" and \
               working_setpoints[0]["temp"] == ECO_TEMPERATURE:
                
                working_setpoints.pop(0)
                logging.debug(f"OPTIMIZATION: Zone '{zone}' Day {day}: Removed 00:00 ECO to preserve previous day's heating.")

            # --- 3. De-Duplication Optimization (Remove redundant state changes) ---
            optimized_setpoints = []
            previous_temp = None
            
            # The previous day's temperature (which rules 00:00 now) is unknown, 
            # so we keep the first setpoint regardless of temperature change.
            for sp in working_setpoints:
                if not optimized_setpoints:
                    optimized_setpoints.append(sp)
                elif sp["temp"] != optimized_setpoints[-1]["temp"]:
                    optimized_setpoints.append(sp)
            
            # --- 4. Conditional 00:00 ECO Insertion (Resilience for empty days) ---
            if not optimized_setpoints:
                # This only happens if a day had no bookings and no cross-midnight event
                optimized_setpoints.append({"time": "00:00", "temp": ECO_TEMPERATURE})
                logging.debug(f"OPTIMIZATION: Zone '{zone}' Day {day}: Added default 00:00 ECO (No events).")
            
            # --- 5. Slot Limiting (Max 6) - Prioritize Comfort ---
            final_setpoints = optimized_setpoints
            
            if len(final_setpoints) > 6:
                num_to_remove = len(final_setpoints) - 6
                indices_to_remove = []
                
                # We need to remove intermediate ECO setpoints to bridge comfort periods (heat ON).
                # Find all intermediate ECO points: those that are NOT the first point AND NOT the last point.
                intermediate_eco_indices = [i for i, sp in enumerate(final_setpoints) 
                                            if sp['temp'] == ECO_TEMPERATURE and 0 < i < len(final_setpoints) - 1]
                
                # Remove intermediate ECO points first (Prioritizing the Heat ON state)
                for i in intermediate_eco_indices:
                    if len(indices_to_remove) < num_to_remove:
                        # Check if removing this ECO point bridges two comfort periods (ON -> OFF -> ON becomes ON -> ON)
                        # We are actually looking for (ON -> OFF) -> ON. Removing OFF leaves ON -> ON
                        if final_setpoints[i-1]['temp'] > ECO_TEMPERATURE and final_setpoints[i+1]['temp'] > ECO_TEMPERATURE:
                            indices_to_remove.append(i)
                        elif final_setpoints[i-1]['temp'] > ECO_TEMPERATURE: # If the previous point was heat ON, remove this OFF point to extend ON time
                            indices_to_remove.append(i)
                    else:
                        break

                # If we still need to remove more, remove ECO points near the start.
                if len(indices_to_remove) < num_to_remove:
                    # Remove the remaining needed points from the top, favoring the removal of ECO points
                    all_eco_indices = [i for i, sp in enumerate(final_setpoints) if sp['temp'] == ECO_TEMPERATURE and i not in indices_to_remove]
                    
                    for i in all_eco_indices:
                        if len(indices_to_remove) < num_to_remove:
                            indices_to_remove.append(i)
                        else:
                            break

                # Rebuild the list without the marked indices
                final_setpoints = [sp for i, sp in enumerate(optimized_setpoints) if i not in indices_to_remove]
                
                logging.warning(f"OPTIMIZATION: Zone '{zone}' Day {day} setpoints exceeded 6. Removed {len(indices_to_remove)} setpoints by consolidating ECO periods. New count: {len(final_setpoints)}.")

            # --- 6. Final De-Duplication and Assignment ---
            # Re-sort/re-optimize one last time as removing a point might create a new duplicate state.
            final_final_setpoints = []
            prev_temp = None
            for sp in final_setpoints:
                if sp["temp"] != prev_temp:
                    final_final_setpoints.append(sp)
                    prev_temp = sp["temp"]
            final_setpoints = final_final_setpoints

            zone_schedule[zone][day] = final_setpoints

            if final_setpoints:
                # PROBE E (Final Merged Schedule)
                logging.debug(f"PROBE E (Final Merged Schedule): Zone '{zone}' Day {day}: {final_setpoints}")
    
    logging.info(f"AGGREGATION END: Successfully generated schedules for {len(zone_schedule)} NeoHub zones.")
    return zone_schedule

def _format_setpoints_for_neohub(
    daily_setpoints: List[Dict[str, Union[str, float]]]
) -> Dict[str, List[Union[str, float, int, bool]]]:
    """
    Takes the aggregated setpoints, pads them to exactly 6 levels, and formats them using the 
    correct NeoHub keys (wake, level1-level4, sleep) to resolve the empty profile issue.
    """
    
    # 1. Prepare setpoints (Max 6)
    setpoints_to_use = daily_setpoints[:len(NEOHUB_SLOTS)] # Ensure max 6 are used
    
    # 2. Robust Padding: If less than 6, fill the remaining slots. (CRITICAL FIX)
    if setpoints_to_use:
        # Use the last valid setpoint (the final ECO time/temp) for padding.
        last_valid_sp = setpoints_to_use[-1] 
    else:
        # Failsafe: Default to 00:00 @ ECO_TEMPERATURE if list is empty.
        last_valid_sp = {"time": "00:00", "temp": ECO_TEMPERATURE}

    # Pad until 6 setpoints are available. This guarantees the correct payload structure.
    while len(setpoints_to_use) < len(NEOHUB_SLOTS):
        setpoints_to_use.append(last_valid_sp)

    # 3. Format and map to NEOHUB_SLOTS
    neohub_schedule_dict = {}
    for i, sp in enumerate(setpoints_to_use):
        slot_name = NEOHUB_SLOTS[i]
        
        # NeoHub format: [time, temperature (1 decimal), sensitivity, enabled (true)]
        neohub_schedule_dict[slot_name] = [
            sp["time"],
            float(f'{sp["temp"]:.1f}'),          # Ensure temperature is float, 1 decimal place
            TEMPERATURE_SENSITIVITY,     
            True                         
        ]
        
    return neohub_schedule_dict

def _validate_neohub_profile(
    profile_data: Dict[str, Dict[str, List[Union[str, float, int, bool]]]], 
    zone_name: str
) -> Tuple[bool, str]:
    """
    Verifies the profile adheres to the 7-day/6-level NeoHub protocol and checks time sequence.
    """
    expected_days = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
    expected_slots = ["wake", "level1", "level2", "level3", "level4", "sleep"]
    
    # Check 1: 7-Day Construct
    # If this fails, the profile is using wrong keys (like integers 0-6).
    if set(profile_data.keys()) != expected_days:
        missing = expected_days - set(profile_data.keys())
        extra = set(profile_data.keys()) - expected_days
        return False, (
            f"Profile is NOT a 7-day construct. Missing days: {missing}, Extra keys: {extra}. "
            f"Expected string keys ('monday', etc.) but likely found integer keys (0-6)."
        )

    # Check 2 & 3: 6-Level Construct and Sequential Time Order
    for day_name in sorted(list(expected_days)):
        daily_schedule = profile_data.get(day_name)
        
        # --- CRITICAL SAFETY CHECK ---
        # If the day mapping in the calling function is wrong, this prevents a crash.
        if daily_schedule is None:
             # This should be unreachable if Check 1 passed, but ensures safety.
            return False, f"Internal Error: Day '{day_name}' missing from profile data structure."

        # Check 2: 6-Level Construct
        if set(daily_schedule.keys()) != set(expected_slots):
            missing = set(expected_slots) - set(daily_schedule.keys())
            extra = set(daily_schedule.keys()) - set(expected_slots)
            return False, (
                f"Day '{day_name}' for zone '{zone_name}' is NOT 6-level. "
                f"Missing slots: {missing}, Extra keys: {extra}"
            )

        # FIX: Reset the previous time for the start of *each* day
        prev_time_str = None 
        
        # Check 3: Sequential Time Order
        for slot_name in expected_slots:
            try:
                time_str = daily_schedule[slot_name][0]
                current_time = datetime.datetime.strptime(time_str, "%H:%M").time()
            except (KeyError, ValueError, IndexError) as e:
                return False, f"Data structure error on day '{day_name}', slot '{slot_name}': {e}"
            
            if prev_time_str:
                prev_time = datetime.datetime.strptime(prev_time_str, "%H:%M").time()
                # Times must be strictly increasing WITHIN THE DAY but can be the same time
                if current_time < prev_time:
                    return False, (
                        f"Time sequencing error on day '{day_name}' for slot '{slot_name}'. "
                        f"Time ({time_str}) must be LATER than the previous slot ({prev_time_str})."
                    )
            prev_time_str = time_str
            
    return True, "Profile is compliant."

async def get_profile_id_by_name(neohub_object: NeoHub, neohub_name: str, profile_name: str) -> Optional[int]:
    """
    Retrieves the numerical profile ID for a given profile name using the GET_PROFILE command,
    handling the response as a SimpleNamespace object.
    """
    logging.info(f"Attempting ID retrieval for existing profile '{profile_name}' using GET_PROFILE...")
    
    # 1. Fetch the raw response (returns a SimpleNamespace object)
    profiles_raw_response = await get_profile(neohub_name, profile_name)
    
    # --- DEBUG PROBES (Retained for one last run if needed) ---
    logging.debug(f"PROBE 1 (Raw Response Type): {type(profiles_raw_response)}")
    # --------------------

    # 2. Check for the correct SimpleNamespace type
    if isinstance(profiles_raw_response, SimpleNamespace):
        
        # Access the PROFILE_ID attribute directly from the SimpleNamespace object.
        # We use getattr() for safe access.
        profile_id = getattr(profiles_raw_response, "PROFILE_ID", None)
        
        if profile_id is not None:
            try:
                # Return the integer ID for the STORE_PROFILE2 command
                final_id = int(profile_id)
                logging.info(f"Successfully retrieved existing profile ID: {final_id} for '{profile_name}'.")
                return final_id
            except ValueError:
                logging.error(f"Found profile ID ('{profile_id}'), but it could not be parsed as an integer.")
                return None
        
        # Fallthrough for SimpleNamespace if PROFILE_ID is missing
        logging.error(f"Key 'PROFILE_ID' not found in the SimpleNamespace object for '{profile_name}'.")
        return None

    # 3. Handle unexpected types (should no longer be the case)
    else:
        logging.error(f"Failed to retrieve valid profile data for '{profile_name}'. Unexpected final data type: {type(profiles_raw_response)}")
        return None
    
async def check_neohub_compatibility(neohub_object: NeoHub, neohub_name: str) -> bool:
    """
    Checks the NeoHub connection, firmware version, and 6-stage profile configuration 
    using the custom send_command utility, passing neohub_name (string key) as required.
    """
    logging.info(f"Checking compatibility for Neohub {neohub_name} using custom send_command...")

    try:
        # Pass neohub_name (the string key) as the first argument to your custom send_command
        command = {"GET_SYSTEM": {}}
        system_info: Optional[Dict[str, Any]] = await send_command(neohub_name, command) 
        
        if system_info is None:
            # Error comes from your send_command (e.g., "Not connected to Neohub")
            logging.error(f"Compatibility check FAILED for {neohub_name}: Did not receive a valid response from GET_SYSTEM or hub not found.")
            return False

        # --- Check 1: Heating Levels ---
        current_levels = system_info.get('HEATING_LEVELS')
        if current_levels != REQUIRED_HEATING_LEVELS:
            logging.error(
                f"Compatibility check FAILED for {neohub_name}: "
                f"Hub is not configured for a {REQUIRED_HEATING_LEVELS}-stage profile (HEATING_LEVELS). "
                f"Current: {current_levels}. Expected: {REQUIRED_HEATING_LEVELS}."
            )
            return False

        # --- Check 2: Firmware Version ---
        try:
            current_firmware = int(system_info.get('HUB_VERSION', 0)) 
        except (ValueError, TypeError):
            current_firmware = 0
            
        if current_firmware < MIN_FIRMWARE_VERSION:
            logging.error(
                f"Compatibility check FAILED for {neohub_name}: "
                f"Firmware version ({current_firmware}) is too old. "
                f"Minimum required ({MIN_FIRMWARE_VERSION}) for this profile type."
            )
            return False
            
    except NeoHubConnectionError as e:
        logging.error(f"Compatibility check FAILED for {neohub_name} due to connection error: {e}")
        return False
    except Exception as e:
        logging.error(f"Compatibility check FAILED for {neohub_name} due to unexpected error: {e}", exc_info=True)
        return False

    logging.info(f"Compatibility check PASSED for {neohub_name}.")
    return True

async def _post_profile_command(neohub_name: str, profile_name: str, profile_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Posts the profile and returns success status and any error reason."""
    
    # We maintain the necessary 3 arguments (neohub_name, profile_name, profile_data)
    result = await store_profile2(neohub_name, profile_name, profile_data)
    
    # store_profile2 should return a success dict or an error dict from _send_raw_profile_command
    if result and result.get("status") == "Success":
        return True, None
    elif result and result.get("neohub_error"):
        error_msg = result["neohub_error"]
        return False, error_msg
    
    # Handle other errors (timeout, format error)
    return False, "Unknown or non-hub-specific error during profile posting."

async def apply_single_zone_profile(
    neohub_object: NeoHub,
    neohub_name: str, 
    zone_name: str, 
    profile_data: Dict[str, Dict[str, List[Union[str, float, int, bool]]]], 
    profile_prefix: str
) -> bool:
    """
    Applies a validated, aggregated weekly profile to a single NeoHub zone.
    It now activates the profile after successful storage by retrieving the ID.
    """

    # 1. --- NEOHUB COMPATIBILITY CHECK (Existing) ---
    if not await check_neohub_compatibility(neohub_object, neohub_name):
        logging.error(f"Skipping profile application for {zone_name}. Neohub {neohub_name} failed firmware/configuration checks.")
        return False

    # 2. --- PROFILE COMPLIANCE CHECK (Existing) ---
    is_compliant, reason = _validate_neohub_profile(profile_data, zone_name)
    if not is_compliant:
        logging.error(
            f"PROFILE COMPLIANCE FAILED for Zone '{zone_name}' ({profile_prefix}): {reason}. "
            f"Profile was NOT sent to NeoHub."
        )
        return False

    profile_name = f"{profile_prefix}_{zone_name}"
    
    # --- STEP A: Check for existing profile ID ---
    profile_id_for_storage = await get_profile_id_by_name(neohub_object, neohub_name, profile_name)
    
    if profile_id_for_storage:
        logging.info(f"Retrieved ID {profile_id_for_storage}. Attempting to update profile by ID...")
    else:
        logging.info(f"Profile '{profile_name}' not found. Attempting to create new profile by name...")

    try:
        # 3. Store/Update the profile
        if profile_id_for_storage:
            # UPDATE PATH: Pass the existing ID to update the profile
            await store_profile2(neohub_name, profile_name, profile_data, profile_id=profile_id_for_storage)
            profile_id_to_activate = profile_id_for_storage
        else:
            # CREATE PATH: OMIT the profile_id argument entirely
            await store_profile2(neohub_name, profile_name, profile_data)
            
            # 4. Retrieve ID if a NEW profile was created
            profile_id_to_activate = await get_profile_id_by_name(neohub_object, neohub_name, profile_name)

            if profile_id_to_activate is None:
                logging.error(f"FATAL: New profile stored successfully for '{zone_name}' but the new ID could not be retrieved for activation.")
                return False

        logging.info(f"Profile successfully stored/updated. Preparing to activate ID {profile_id_to_activate} on '{zone_name}'.")

        # 5. --- ACTIVATE THE PROFILE ---
        # profile_id_to_activate is now guaranteed to hold a valid ID
        return await activate_profile_on_zones(neohub_name, profile_id_to_activate, zone_name)

    except Exception as e:
        logging.error(f"Failed to execute custom profile command for '{zone_name}': {e}", exc_info=True)
        return False
    
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

async def store_profile2(neohub_name: str, profile_name: str, profile_data: Dict[str, Any], profile_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Stores a heating profile on the Neohub, passing a Python dictionary structure directly.
    Accepts an optional profile_id for updates.
    """
    logging.info(f"Storing profile {profile_name} (ID: {profile_id if profile_id else 'New'}) on Neohub {neohub_name}")

    # 1. CREATE THE COMMAND PAYLOAD (clean Python dict with float temps and P_TYPE)
    inner_payload = {
        "STORE_PROFILE2": {
            "name": profile_name,
            "P_TYPE": 0, # 0 for Heating Profile
            "info": profile_data
        }
    }

    # 2. DEBUGGING ECHO
    logging.debug(f"DEBUG: FINAL Python Dict Payload: {json.dumps(inner_payload)}")

    # 3. SEND THE COMMAND DICT DIRECTLY, PASSING THE ID
    # The ID is passed here, but only injected into the command payload inside send_command's raw function.
    response = await send_command(neohub_name, inner_payload, profile_id=profile_id)
    return response

async def send_command(neohub_name: str, command: Dict[str, Any], profile_id: Optional[int] = None) -> Optional[Any]:
    """
    Sends a command to the Neohub, using a custom raw send for complex profile commands.
    If profile_id is provided, it uses the specialized update function.
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
        # --- LOGIC: Route based on profile_id ---
        if profile_id is not None:
            logging.debug(f"PROBE 3 (SC): Delegating to _send_raw_profile_update_command for {neohub_name}.")
            return await _send_raw_profile_update_command(hub, command, profile_id)
        else:
            logging.debug(f"PROBE 3 (SC): Delegating to _send_raw_profile_command for {neohub_name}.")
            return await _send_raw_profile_command(hub, command)
        # --- END LOGIC ---
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

async def _send_raw_profile_command(hub, command: Dict[str, Any]) -> Optional[Any]:
    """
    Manually constructs, sends, and waits for the response for the STORE_PROFILE2 
    command.
    """
    global _command_id_counter
    
    # --- PROBE A (RAW): Entering _send_raw_profile_command. ---
    logging.debug(f"PROBE A (RAW): Entering _send_raw_profile_command. Relying on existing connection.")
    
    # --- PROBE F: Final Check before payload construction ---
    logging.debug(f"PROBE F (RAW): Proceeding with raw send on existing connection.")
    
    hub_token = getattr(hub, '_token', None)
    hub_client = getattr(hub, '_client', None) 
    
    if not hub_token or not hub_client:
        logging.error("Could not access private token or client (_token or _client) for raw send.")
        return None

    try: 
        # 0. Preparation
        command_to_send = command
        
        # 1. Serialize the command
        command_id = next(_command_id_counter)
        command_value_str = json.dumps(command_to_send, separators=(',', ':'))

        # 2. **HACK 1: Convert all double quotes to single quotes** (crucial for unquoted true/false)
        command_value_str_hacked = command_value_str.replace('"', "'")
        
        # 3. **HACK 2: Manually construct the INNER_MESSAGE string**
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
             raise AttributeError("Could not find internal mechanisms needed for raw send/receive. Connection may be closed.")
        
        future: asyncio.Future[Any] = asyncio.Future()
        pending_requests[command_id] = future

        logging.debug(f"Raw Sending: {final_payload_string}")
        
        # 7. Send and wait
        await raw_ws_send(final_payload_string)
        response_dict = await asyncio.wait_for(future, timeout=request_timeout)
        
        # 8. Process the response
        logging.debug(f"Received STORE_PROFILE2 response (COMMANDID {command_id}): {response_dict}")

        # CRITICAL FIX 1: Add safeguard against 'str' object has no attribute 'get' error.
        # This occurs if the future resolves with an unprocessed string instead of a dict.
        if isinstance(response_dict, str):
            try:
                response_dict = json.loads(response_dict)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode response string for command {command_id}: {response_dict}")
                raise ValueError("Response was a string but failed JSON decoding.") from e

        if not isinstance(response_dict, dict):
            logging.error(f"Response for command {command_id} is not a dictionary after processing: {response_dict}")
            return {"command_id": command_id, "status": "Format Error"}
        
        # CRITICAL FIX 2: Handle the SUCCESS response format based on logs: {"ID":X, "result":"profile created"}
        if response_dict.get("result") == "profile created" and "ID" in response_dict:
            profile_id = response_dict["ID"]
            logging.info(f"Successfully stored profile with ID: {profile_id}")
            return {"command_id": command_id, "status": "Success", "profile_id": profile_id}
        
        # Handle the secondary success/error format (original library logic)
        profile_data = response_dict.get("STORE_PROFILE2")

        if profile_data and isinstance(profile_data, dict) and "PROFILE_ID" in profile_data:
             profile_id = profile_data["PROFILE_ID"]
             logging.info(f"Successfully stored profile with ID: {profile_id}")
             return {"command_id": command_id, "status": "Success", "profile_id": profile_id}
        elif "error" in response_dict:
             logging.error(f"Neohub returned error for command {command_id}: {response_dict['error']}")
             return {"command_id": command_id, "status": "Error", "neohub_error": response_dict['error']}
        else:
             logging.error(f"Neohub returned unhandled response for command {command_id}: {response_dict}. Check app/device for submission status.")
             return {"command_id": command_id, "status": "Unexpected Response", "response": response_dict}

    except asyncio.TimeoutError:
        logging.error(f"Timeout waiting for response for command {command_id}.")
        return {"command_id": command_id, "status": "Timeout"}
    except Exception as e:
        logging.error(f"Error during raw WebSocket send/receive for profile command: {e}")
        return None
    finally:
        # Clean up the pending request
        if pending_requests and 'command_id' in locals() and command_id in pending_requests:
             del pending_requests[command_id]

async def _send_raw_profile_update_command(hub, command: Dict[str, Any], profile_id: int) -> Optional[Dict[str, Any]]:
    """
    Manually constructs, sends, and waits for the response for the STORE_PROFILE2 
    command specifically for UPDATING an existing profile by ID.
    
    The ID is inserted before serialization to follow the same pattern as the original
    function's quirky serialization process.
    """
    global _command_id_counter
    
    logging.debug(f"PROBE A (UPDATE RAW): Entering _send_raw_profile_update_command. ID: {profile_id}. Relying on existing connection.")
    
    hub_token = getattr(hub, '_token', None)
    hub_client = getattr(hub, '_client', None) 
    
    if not hub_token or not hub_client:
        logging.error("Could not access private token or client (_token or _client) for raw send.")
        return None

    try: 
        # 0. Preparation
        command_to_send = command
        command_id = next(_command_id_counter)
        
        # Ensure the command has the STORE_PROFILE2 key
        if 'STORE_PROFILE2' not in command_to_send or not isinstance(command_to_send['STORE_PROFILE2'], dict):
             logging.error("Invalid command format provided to profile update function.")
             return None

        # 1. CRITICAL: Inject the ID into the dictionary BEFORE serialization
        command_to_send['STORE_PROFILE2']['ID'] = profile_id
        logging.debug(f"PROBE ID (INJECTED): ID {profile_id} injected into command payload.")
        
        
        # 2. Serialize the command (ORIGINAL LOGIC)
        command_value_str = json.dumps(command_to_send, separators=(',', ':'))

        # **HACK 1: Convert all double quotes to single quotes** (crucial for unquoted true/false)
        command_value_str_hacked = command_value_str.replace('"', "'")
        
        
        # 3. **HACK 2: Manually construct the INNER_MESSAGE string**
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
             raise AttributeError("Could not find internal mechanisms needed for raw send/receive. Connection may be closed.")
        
        future: asyncio.Future[Any] = asyncio.Future()
        pending_requests[command_id] = future

        logging.debug(f"Raw Sending: {final_payload_string}")
        
        # 7. Send and wait
        await raw_ws_send(final_payload_string)
        response_dict_raw = await asyncio.wait_for(future, timeout=request_timeout)
        
        # 8. Process the response (Handling the `hm_set_command_response` format)
        logging.debug(f"Received STORE_PROFILE2 response (COMMANDID {command_id}): {response_dict_raw}")
        
        # Standardize response structure
        response_data = response_dict_raw
        if isinstance(response_dict_raw, dict) and 'response' in response_dict_raw:
            try:
                # The response field is a JSON string of the error or success payload
                response_data = json.loads(response_dict_raw['response'])
            except json.JSONDecodeError:
                # If it's not JSON, assume it's the raw error string
                response_data = response_dict_raw['response']
        elif isinstance(response_dict_raw, str):
            try:
                response_data = json.loads(response_dict_raw)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode response string for command {command_id}: {response_dict_raw}")
                raise ValueError("Response was a string but failed JSON decoding.") from e
        
        if not isinstance(response_data, dict):
            logging.error(f"Final response for command {command_id} is not a dictionary: {response_data}")
            return {"command_id": command_id, "status": "Format Error"}
        
        # --- RESPONSE CHECKING ---
        
        # 8a. Handle success response for new profile (unlikely for this function, but safe)
        if response_data.get("result") == "profile created" and "ID" in response_data:
            profile_id_res = response_data["ID"]
            logging.info(f"Successfully stored profile with ID: {profile_id_res}")
            return {"command_id": command_id, "status": "Success", "profile_id": profile_id_res}
        
        # 8b. Handle success response for existing profile (update) - often just a success signal or echoed ID
        if "PROFILE_ID" in response_data and response_data.get("P_TYPE") is not None:
             profile_id_res = response_data["PROFILE_ID"]
             logging.info(f"Successfully updated profile ID: {profile_id_res}")
             return {"command_id": command_id, "status": "Success", "profile_id": profile_id_res}

        # 8c. Handle error
        elif "error" in response_data:
            logging.error(f"Neohub returned error for command {command_id}: {response_data['error']}")
            return {"command_id": command_id, "status": "Error", "neohub_error": response_data['error']}
        
        # 8d. Handle unhandled responses
        else:
            logging.error(f"Neohub returned unhandled response for command {command_id}: {response_data}. Check app/device for submission status.")
            return {"command_id": command_id, "status": "Unexpected Response", "response": response_data}

    except asyncio.TimeoutError:
        logging.error(f"Timeout waiting for response for command {command_id}.")
        return {"command_id": command_id, "status": "Timeout"}
    except Exception as e:
        # Catch errors raised inside the function (like ValueError for decoding)
        logging.error(f"Error during raw WebSocket send/receive for profile command: {type(e).__name__}: {e}")
        return None
    finally:
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

def get_bookings_and_locations() -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """Fetches all bookings and resources from ChurchSuite."""
    if not CHURCHSUITE_URL:
        logging.error("CHURCHSUITE_URL is not configured. Cannot fetch bookings.")
        return None
    
    try:
        # NOTE: This uses the URL directly, assuming it points to a downloadable JSON resource.
        response = requests.get(CHURCHSUITE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # LOGGING IMPROVEMENT: Check the pagination block for total results
        num_results = data.get("pagination", {}).get("num_results", 0)
        logging.info(f"Successfully downloaded ChurchSuite data: {num_results} total bookings found.")
        
        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch ChurchSuite data from {CHURCHSUITE_URL}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse ChurchSuite response as JSON: {e}")
        return None

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

async def check_neohub_compatibility(neohub_object: NeoHub, neohub_name: str) -> bool:
    """
    Checks the NeoHub connection, firmware version, and 6-stage profile configuration 
    using a retry mechanism to stabilize the connection before the first command.
    """
    logging.info(f"Checking compatibility for Neohub {neohub_name} using custom send_command...")

    command = {"GET_SYSTEM": {}}
    
    for attempt in range(MAX_ATTEMPTS):
        if attempt > 0:
            # Apply delay before retry (0.5s before 2nd, 2.0s before 3rd)
            delay = RETRY_DELAYS[attempt - 1]
            logging.warning(f"Compatibility check failed on attempt {attempt}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            
        try:
            # We assume send_command handles the raw send and uses the global _command_id_counter.
            system_info: Optional[Dict[str, Any]] = await send_command(neohub_name, command) 

            if system_info is None:
                # If send_command returns None, something fundamental failed (e.g., ConnectionError)
                # The loop will proceed to retry unless it's the last attempt.
                raise NeoHubConnectionError("No valid response received from GET_SYSTEM.")
                
            # --- CRITICAL FIX for SimpleNamespace ---
            # Convert to a dictionary if necessary to use .get()
            if not isinstance(system_info, dict):
                try:
                    system_info = vars(system_info) 
                except TypeError:
                    logging.error(f"Compatibility check FAILED for {neohub_name}: system_info is an unexpected object type.")
                    # Return False immediately if the object is corrupted
                    return False
            # ----------------------------------------
            
            # --- Check 1: Heating Levels ---
            current_levels = system_info.get('HEATING_LEVELS')
            if current_levels != REQUIRED_HEATING_LEVELS:
                 # Success, but wrong config. Fail immediately (no retry needed for config error).
                 logging.error(f"Compatibility check FAILED for {neohub_name}: Hub is not configured for a {REQUIRED_HEATING_LEVELS}-stage profile (HEATING_LEVELS). Current: {current_levels}. Expected: {REQUIRED_HEATING_LEVELS}.")
                 return False

            # --- Check 2: Firmware Version ---
            # (Your existing firmware check logic goes here)
            try:
                current_firmware = int(system_info.get('HUB_VERSION', 0)) 
            except (ValueError, TypeError):
                current_firmware = 0
                
            if current_firmware < MIN_FIRMWARE_VERSION:
                logging.error(f"Compatibility check FAILED for {neohub_name}: Firmware version ({current_firmware}) is too old. Minimum required ({MIN_FIRMWARE_VERSION}) for this profile type.")
                # Success, but wrong firmware. Fail immediately.
                return False

            # If all checks pass, break the loop and succeed
            logging.info(f"Compatibility check PASSED for {neohub_name}.")
            return True

        except (NeoHubConnectionError, Exception) as e:
            # Log the specific error that triggered the retry/failure
            if attempt == MAX_ATTEMPTS - 1:
                 logging.error(f"Compatibility check FAILED permanently for {neohub_name} after {MAX_ATTEMPTS} attempts: {e}")
                 return False
            else:
                 logging.debug(f"Attempt {attempt+1} failed with error: {e}")
                 # Continue to the next iteration for retry

    # Should be unreachable, but here for completeness
    return False

async def apply_aggregated_schedules(
    aggregated_schedules: Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]], 
    profile_prefix: str, 
    config: Dict[str, Any],
    zone_to_neohub_map: Dict[str, str]
) -> None:
    """
    Takes aggregated weekly setpoints, formats them for the NeoHub, validates them,
    and applies them to the corresponding NeoHub zones.
    """
    
    tasks = []

    # FIX: Define the required mapping from integer day index to string day name.
    DAY_MAPPING = {
        0: "monday", 
        1: "tuesday", 
        2: "wednesday", 
        3: "thursday",
        4: "friday", 
        5: "saturday", 
        6: "sunday"
    }

    for zone_name, daily_schedules in aggregated_schedules.items(): 
        
        neohub_name = zone_to_neohub_map.get(zone_name)
        # Assuming 'hubs' is a globally available dictionary mapping neohub names to connected NeoHub objects
        neohub_object = hubs.get(neohub_name) 

        if not neohub_object:
            logging.error(f"Cannot apply schedule for zone '{zone_name}': Neohub '{neohub_name}' not connected or mapped. Skipping.")
            continue
            
        profile_data = {}
        
        # daily_schedules.items() yields (day_index: int, setpoints_list: list)
        for day_index, setpoints_list in daily_schedules.items():
            
            # CRITICAL FIX APPLIED HERE: Convert the integer index to the required string name
            day_name = DAY_MAPPING.get(day_index)
            
            if not day_name:
                logging.warning(f"Invalid day index found: {day_index} for zone {zone_name}. Skipping day.")
                continue

            # Format the daily setpoints
            formatted_daily_schedule = _format_setpoints_for_neohub(setpoints_list)
            
            # The profile data is now correctly keyed by the string day name
            profile_data[day_name] = formatted_daily_schedule
        
        if not profile_data:
            logging.warning(f"No profile data generated for zone {zone_name}. Skipping.")
            continue

        logging.debug(f"NEOHUB PAYLOAD READY for Zone '{zone_name}'.")
        
        # Add a task to apply the profile (this calls the function with the validation check)
        tasks.append(
            apply_single_zone_profile(
                neohub_object,
                neohub_name, 
                zone_name, 
                profile_data, 
                profile_prefix
            )
        )

    if tasks:
        logging.info(f"Applying {len(tasks)} zone profiles for {profile_prefix}.")
        await asyncio.gather(*tasks)
    else:
        logging.warning(f"No profiles generated or applied for {profile_prefix}.")

async def activate_profile_on_zones(neohub_name: str, profile_id: int, zone_name: str) -> bool:
    """
    Activates a specific profile ID on one or more heating zones using the 
    RUN_PROFILE_ID command.

    Args:
        neohub_name (str): The name of the NeoHub client is connected to.
        profile_id (int): The ID of the profile to be activated.
        zone_name (str): The name of the zone (or a list of zones) to apply the profile to.
                         (We assume the zone name maps directly to the neoStat name/identifier).
                         
    Returns:
        bool: True if the command was sent successfully and the response indicates success.
    """
    logging.info(f"Attempting to activate Profile ID {profile_id} on zone(s): '{zone_name}' via {neohub_name}.")

    try:
        # The required payload format for the RUN_PROFILE_ID command:
        # {“RUN_PROFILE_ID”:[25,"Kitchen","Lounge"]}
        
        # Build the command payload structure
        # Note: The API docs show a list of zones, so we wrap the single zone_name in a list.
        inner_command = {
            "RUN_PROFILE_ID": [
                profile_id,
                zone_name
            ]
        }
        
        # Use your existing send_command wrapper. We assume it correctly handles 
        # wrapping the inner_command into the full WebSocket payload.
        response: Optional[Dict[str, Any]] = await send_command(neohub_name, inner_command)

        if response and response.get('result') == 'ok':
            logging.info(f"Successfully activated Profile ID {profile_id} on zone(s) '{zone_name}'.")
            return True
        else:
            logging.error(f"Failed to activate profile {profile_id} on '{zone_name}'. Hub response: {response}")
            return False

    except Exception as e:
        logging.error(f"Error during RUN_PROFILE_ID command for zone '{zone_name}' on {neohub_name}: {e}")
        return False

async def update_heating_schedule() -> None:
    """
    Updates the heating schedule based on upcoming bookings,
    aggregating schedules by neohub and zone using a rolling 7-day window.
    """
    # --- START CRITICAL LIFECYCLE MANAGEMENT ---
    
    # 1. Reset the high-ID application counter (NEW LINES)
    global _command_id_counter
    _command_id_counter.reset()

    logging.info("--- STARTING HEATING SCHEDULE UPDATE PROCESS (7-Day Rolling Window) ---")
    global config
    
    # 1. Configuration Validation
    if config is None:
        logging.error("Configuration not loaded. Exiting.")
        return
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        return
    
    # NEW: Build the centralized configuration map
    zone_to_neohub_map = build_zone_to_neohub_map(config)
    
    # 2. Timezone and Rolling Window Calculation
    location_timezone_name = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
    try:
        location_timezone = pytz.timezone(location_timezone_name)
    except pytz.exceptions.UnknownTimeZoneError:
        logging.error(
            f"Timezone '{location_timezone_name}' is invalid. Defaulting to Europe/London."
        )
        location_timezone = pytz.timezone("Europe/London")

    # The current moment, naive time in the location's timezone
    now_naive = datetime.datetime.now(location_timezone).replace(tzinfo=None)

    # PROFILE 1: Covers the next 7 days (the current rolling week)
    profile_1_end = now_naive + datetime.timedelta(days=7)
    # PROFILE 2: Covers days 8 through 14
    profile_2_end = now_naive + datetime.timedelta(days=14)
    
    logging.info(f"Current Date/Time: {now_naive.strftime('%Y-%m-%d %H:%M')}")
    logging.info(f"Profile 1 (Current Week) Range: NOW to {profile_1_end.strftime('%Y-%m-%d %H:%M')}")
    logging.info(f"Profile 2 (Next Week) Range: {profile_1_end.strftime('%Y-%m-%d %H:%M')} to {profile_2_end.strftime('%Y-%m-%d %H:%M')}")


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

        # Create a map to resolve resource_id to location name
        resource_id_to_name = {r.get('id'): r.get('name') for r in resources if r.get('id') and r.get('name')}
        logging.debug(f"RESOURCE MAP: Created ID to Name map with {len(resource_id_to_name)} entries.")
        
        # 2. Filter Bookings by Rolling Window
        profile_1_bookings = []
        profile_2_bookings = []

        for booking in booked_resources:
            start_time_str = booking.get("starts_at")
            end_time_str = booking.get("ends_at")
            resource_id = booking.get("resource_id", "unknown")
            
            if start_time_str and end_time_str:
                try:
                    # Parse all times to local naive time for comparison
                    start_dt_utc = dateutil.parser.parse(start_time_str)
                    end_dt_utc = dateutil.parser.parse(end_time_str)
                    
                    # Convert to local time, remove TZ info
                    local_start_dt = start_dt_utc.astimezone(location_timezone).replace(tzinfo=None)
                    local_end_dt = end_dt_utc.astimezone(location_timezone).replace(tzinfo=None)
                    
                    # CRITICAL: Lapsed Event Check
                    # If the event has already ENDED, we skip it. This prevents deleting an active schedule
                    # if the start time was in the past, but the end time is in the future.
                    if local_end_dt < now_naive:
                        logging.debug(f"Skipping booking ID {booking.get('id')}: Event finished at {local_end_dt}.")
                        continue
                    
                    # PROBE B (Filtering Check) - Renamed for clarity on the first event being processed
                    if LOGGING_LEVEL == "DEBUG" and not (profile_1_bookings or profile_2_bookings):
                        logging.debug(f"PROBE B (Filtering Check): First un-lapsed booking starts: {local_start_dt}. Ends: {local_end_dt}. Now: {now_naive}")

                    # PROFILE 1: Events that start in the next 7 days (or are currently running)
                    if local_start_dt < profile_1_end:
                        profile_1_bookings.append(booking)
                    
                    # PROFILE 2: Events that start between day 7 and day 14
                    elif local_start_dt < profile_2_end:
                        profile_2_bookings.append(booking)
                        
                except dateutil.parser.ParserError as e:
                    logging.error(f"Failed to parse datetime for booking ID {booking.get('id', 'unknown')}: {e}")
            else:
                logging.warning(f"Booking with resource ID {resource_id} has missing start/end time.")

        logging.info(f"Filtered Bookings: Profile 1 (Next 7 days): {len(profile_1_bookings)}, Profile 2 (Days 8-14): {len(profile_2_bookings)}")
        
        # PROBE C: Log the resulting lists
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"PROBE C (Filtered): Profile 1 Bookings ({len(profile_1_bookings)}): {profile_1_bookings}")
            logging.debug(f"PROBE C (Filtered): Profile 2 Bookings ({len(profile_2_bookings)}): {profile_2_bookings}")


        # 3. Get External Temperature
        external_temperature = get_external_temperature()
        logging.info(f"Fetched external temperature: {external_temperature}°C")

        # 4. AGGREGATE SCHEDULES BY ZONE (The Critical Step)
        aggregated_p1_schedules = create_aggregated_schedule(
            profile_1_bookings, 
            external_temperature, 
            config,
            resource_id_to_name
        )
        logging.info(f"AGGREGATION RESULT (Profile 1): {len(aggregated_p1_schedules)} final locations/zones scheduled.")
        
        aggregated_p2_schedules = create_aggregated_schedule(
            profile_2_bookings, 
            external_temperature, 
            config,
            resource_id_to_name
        )
        logging.info(f"AGGREGATION RESULT (Profile 2): {len(aggregated_p2_schedules)} final locations/zones scheduled.")

        # PROBE D: Log the final aggregated schedules before application
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"PROBE D (Final Aggregation): Profile 1: {aggregated_p1_schedules}")
            logging.debug(f"PROBE D (Final Aggregation): Profile 2: {aggregated_p2_schedules}")


        # 5. APPLY AGGREGATED SCHEDULES
        # The profile names remain "Current Week" and "Next Week" for the NeoHub hardware
        await apply_aggregated_schedules(
            aggregated_p1_schedules, "Current Week", config, zone_to_neohub_map
        )
        await apply_aggregated_schedules(
            aggregated_p2_schedules, "Next Week", config, zone_to_neohub_map
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