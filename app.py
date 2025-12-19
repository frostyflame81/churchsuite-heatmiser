import asyncio
import datetime
import json
import logging
import logging.handlers
import time
import itertools
import requests # type: ignore
from apscheduler.schedulers.asyncio import AsyncIOScheduler # type: ignore
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
WEATHER_FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast"
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
TIMEZONE = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
T_COLD_THRESHOLD = 10.0 # Threshold for scaling preheat time (10°C)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")
MIN_FIRMWARE_VERSION = 2079
REQUIRED_HEATING_LEVELS = 6
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()  # Get logging level from env
# Set up logging
LOG_DIR = "/app/logs"
LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")
os.makedirs(LOG_DIR, exist_ok=True)

# Format: Timestamp | Source | Level | Message
log_formatter = logging.Formatter('%(asctime)s | scheduler | %(levelname)s | %(message)s')

file_handler = logging.handlers.TimedRotatingFileHandler(LOG_FILE, when="D", interval=1, backupCount=7)
file_handler.setFormatter(log_formatter)

# Set the root logger to DEBUG so the file captures everything
# The GUI will handle the visual filtering
logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, logging.StreamHandler()])
logger = logging.getLogger("scheduler")

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

#IPC Constants
MANUAL_RUN_FLAG = '/tmp/manual_run_flag'
CONFIG_RELOAD_FLAG = '/tmp/config_reload_flag'
SCHEDULER_STATUS_FILE = '/tmp/scheduler_status.json'

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

def reload_config_from_disk():
    """Reads configuration from the file path defined by CONFIG_FILE environment variable.
    Preserves existing configuration keys (like NEOHUB details) that were originally 
    loaded from environment variables and are not expected to be in the config file.
    """
    global config
    config_file_path = os.environ.get("CONFIG_FILE")
    
    # 1. Safely store the existing, environment-derived configuration keys (e.g., 'neohubs').
    existing_env_keys: Dict[str, Any] = {}
    if config:
        # Assuming 'locations' is the key users primarily edit in config.json.
        # We store all other top-level keys, which hold the environment-derived secrets.
        for key in list(config.keys()):
            if key != 'locations':
                existing_env_keys[key] = config[key]

    if config_file_path and os.path.exists(config_file_path):
        try:
            with open(config_file_path, 'r') as f:
                new_config_data = json.load(f)
                
                # 2. Overwrite the global config with new data from the file (e.g., 'locations').
                # We use clear() and update() to replace contents safely.
                if config is None:
                    config = {}
                config.clear()
                config.update(new_config_data)
                
                # 3. Restore the preserved, environment-derived configurations.
                # This restores keys like 'neohubs' (which holds address/token) back into the config.
                config.update(existing_env_keys)

                logging.info(f"Configuration successfully reloaded and merged from {config_file_path}. Preserved keys: {list(existing_env_keys.keys())}")
                
        except Exception as e:
            logging.error(f"Failed to reload config file {config_file_path}: {e}")
    else:
        logging.error("Cannot reload config: CONFIG_FILE path is not set or file does not exist.")

def write_status_file(status_data: Dict[str, Any]):
    """Writes the detailed status report to a temporary JSON file."""
    try:
        with open(SCHEDULER_STATUS_FILE, 'w') as f:
            json.dump(status_data, f, indent=4)
        logging.info(f"Scheduler status saved to {SCHEDULER_STATUS_FILE}")
    except Exception as e:
        logging.error(f"Failed to write scheduler status file: {e}")

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
    neohub_name: str, 
    zone_name: str, 
    time_since_minutes: float, 
    forecast_temp: float, 
    event_temperature: float
) -> Tuple[float, float]:
    """
    Calculates the required preheat minutes for a single zone/event based on 
    simulated temperature decay and external forecast.

    Returns: (preheat_minutes: float, T_start_simulated: float)
    """
    # 1. Calculate the simulated internal start temperature (T_start)
    T_start_simulated = calculate_simulated_start_temp(
        zone_name=zone_name,
        neohub_name=neohub_name,
        time_since_minutes=time_since_minutes,
        forecast_temp=forecast_temp
    )

    # 2. Calculate the required preheat duration based on T_start and forecast
    preheat_minutes = calculate_preheat_duration(
        T_target=event_temperature,
        T_start=T_start_simulated,
        T_forecast=forecast_temp
    )
    
    return preheat_minutes, T_start_simulated

def create_aggregated_schedule(
    upcoming_events: List[Dict[str, Any]], 
    external_temperature: Optional[float], # Renamed and kept for placeholder compatibility
    config: Dict[str, Any],
    resource_id_to_name: Dict[int, str],
    zone_statuses: Dict[str, Dict[str, Dict[str, Union[int, float, str]]]] # NEW required parameter
) -> Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]]:
    """
    Processes ChurchSuite bookings to create a single, aggregated heating schedule 
    for each NeoHub Zone by consolidating overlapping periods.
    """
    
    # --- HELPER FUNCTIONS (Defined locally for robust time arithmetic) ---
    def _time_string_to_minutes(time_str: str) -> int:
        h, m = map(int, time_str.split(':'))
        return h * 60 + m

    def _minutes_to_time_string(minutes: int) -> str:
        minutes %= (24 * 60)
        h = minutes // 60
        m = minutes % 60
        return f"{h:02d}:{m:02d}"
        
    def _consolidate_periods(periods: List[Dict[str, Any]], eco_temp: float) -> List[Dict[str, Any]]:
        """Consolidates a list of heating periods into a minimal, non-overlapping set."""
        if not periods:
            return []

        # 1. Sort periods by start time
        periods.sort(key=lambda p: _time_string_to_minutes(p["start"]))
        
        merged = []
        
        # Initialize the first merged period using minutes
        current_start_min = _time_string_to_minutes(periods[0]["start"])
        current_end_min = _time_string_to_minutes(periods[0]["end"])
        current_temp = periods[0]["temp"]

        for i in range(1, len(periods)):
            next_period = periods[i]
            next_start_min = _time_string_to_minutes(next_period["start"])
            next_end_min = _time_string_to_minutes(next_period["end"])
            
            # Check for overlap or touch: next start is before or equal to current end
            if next_start_min <= current_end_min:
                # Overlap or touch: Merge them
                current_end_min = max(current_end_min, next_end_min)
                current_temp = max(current_temp, next_period["temp"]) # Take the highest required temp
            else:
                # Gap found: Finalize the current merged period
                merged.append({
                    "start": _minutes_to_time_string(current_start_min),
                    "end": _minutes_to_time_string(current_end_min),
                    "temp": current_temp
                })
                
                # Start a new merged period
                current_start_min = next_start_min
                current_end_min = next_end_min
                current_temp = next_period["temp"]
                
        # Add the last merged period
        merged.append({
            "start": _minutes_to_time_string(current_start_min),
            "end": _minutes_to_time_string(current_end_min),
            "temp": current_temp
        })
        
        return merged
    # --- END HELPER FUNCTIONS ---


    # Schedule structure now holds Periods: { zone: { day: [ {start: str, end: str, temp: float} ] } }
    zone_schedule: Dict[str, Dict[int, List[Dict[str, Union[str, float]]]]] = {}
    # Fetch ECO_TEMPERATURE from global settings
    eco_temp = config["global_settings"].get("ECO_TEMPERATURE", 12.0) # Default if missing
    # ... (Initialization logic remains the same) ...
    logging.info(f"AGGREGATION START: Processing {len(upcoming_events)} bookings.") # Changed 'bookings' to 'upcoming_events'
    
    # 1. Initialize schedule structure for all zones/days
    for location_name, location_config in config.get("locations", {}).items():
        for zone_name in location_config.get("zones", []):
            if zone_name not in zone_schedule:
                zone_schedule[zone_name] = {i: [] for i in range(7)}
    
    # Handle empty bookings list
    if not upcoming_events:
        logging.info("AGGREGATION END: Bookings list was empty. Returning fully optimized ECO default schedule.")
        # Create a compliant 6-slot ECO schedule for all days/zones
        eco_schedule = [{'time': '00:00', 'temp': eco_temp}] * 6
        for zone, daily_schedule in zone_schedule.items():
            for day in range(7):
                zone_schedule[zone][day] = eco_schedule
        return zone_schedule
    
    # --- Start main booking loop: GENERATE PERIODS ---
    for booking in upcoming_events: # Changed 'bookings' to 'upcoming_events'
        booking_id = booking.get("id", "N/A")
        resource_id = booking.get("resource_id")
        
        # ... (Unchanged pre-processing logic to get location, config, etc.) ...
        location_name = resource_id_to_name.get(resource_id)
        if not location_name or location_name not in config["locations"]:
            continue
        location_config = config["locations"][location_name]
        neohub_name = location_config['neohub'] # Added for status reporting
        zone_names = location_config.get("zones", [])

        try:
            start_dt_utc = dateutil.parser.parse(booking["starts_at"])
            end_dt_utc = dateutil.parser.parse(booking["ends_at"])
            
            local_tz = pytz.timezone(TIMEZONE) 
            start_dt_local = start_dt_utc.astimezone(local_tz).replace(tzinfo=None)
            end_dt_local = end_dt_utc.astimezone(local_tz).replace(tzinfo=None)
            target_temp = location_config.get("default_temp", config["global_settings"].get
                ("DEFAULT_TEMPERATURE", 18.0 # Default if missing
                    ))
            
            for zone_name in zone_names:
                
                # --- NEW LOGIC: DYNAMIC PREHEAT EXTRACTION AND REPORTING ---
                decay_metrics = booking.get('decay_metrics', {}).get(zone_name)
                
                # Initialize reporting structure for this zone (required for both success and fail)
                zone_statuses.setdefault(neohub_name, {}).setdefault(zone_name, {})
                
                min_preheat = config["global_settings"].get("PREHEAT_TIME_MINUTES", 30.0)

                if not decay_metrics:
                    # Use the minimum safe preheat time as a fallback
                    logging.warning(f"Decay metrics missing for {zone_name} in booking ID {booking_id}. Falling back to default {min_preheat} minutes preheat.")
                    preheat_to_use = min_preheat
                    
                    # Update reporting for zones that fail to get decay metrics
                    zone_statuses[neohub_name][zone_name]['AGGREGATE_PROFILE'] = 0
                    zone_statuses[neohub_name][zone_name]['preheat_minutes'] = min_preheat

                else:
                    preheat_to_use = decay_metrics.get("preheat_minutes", min_preheat)
                    T_start_simulated = decay_metrics.get("T_start_simulated")
                    
                    # CRITICAL REPORTING: Flag successful aggregation
                    zone_statuses[neohub_name][zone_name]['AGGREGATE_PROFILE'] = 1
                    zone_statuses[neohub_name][zone_name]['T_start_simulated'] = round(T_start_simulated, 1)
                    zone_statuses[neohub_name][zone_name]['preheat_minutes'] = round(preheat_to_use, 1)

                
                preheat_start_dt_local = start_dt_local - datetime.timedelta(minutes=preheat_to_use)
                
                # --- CRITICAL: PERIOD GENERATION (Handles Midnight Spanning) ---
                
                preheat_start_day = preheat_start_dt_local.weekday()
                event_end_day = end_dt_local.weekday()
                
                # Case 1: Period starts and ends on the same calendar day
                if preheat_start_dt_local.date() == end_dt_local.date():
                    
                    period = {
                        "start": preheat_start_dt_local.strftime("%H:%M"),
                        "end": end_dt_local.strftime("%H:%M"),
                        "temp": target_temp
                    }
                    zone_schedule[zone_name][preheat_start_day].append(period)

                # Case 2: Period spans midnight (starts on one day, ends on the next)
                else:
                    # Period 1: Start time until 23:59 on the start day
                    period_1 = {
                        "start": preheat_start_dt_local.strftime("%H:%M"),
                        "end": "23:59", # Using 23:59 is safer than 00:00 for the end point of the day
                        "temp": target_temp
                    }
                    zone_schedule[zone_name][preheat_start_day].append(period_1)

                    # Period 2: 00:00 until end time on the end day
                    period_2 = {
                        "start": "00:00",
                        "end": end_dt_local.strftime("%H:%M"),
                        "temp": target_temp
                    }
                    zone_schedule[zone_name][event_end_day].append(period_2)
                    
                    logging.debug(
                        f"PROBE H (Cross-Midnight Period): Zone '{zone_name}' ID {booking_id} split from "
                        f"{period_1['start']} (Day {preheat_start_day}) to "
                        f"{period_2['end']} (Day {event_end_day})."
                    )

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logging.error(f"Error processing booking for {location_name} (ID {booking_id}): {e}.", exc_info=True)
            continue
            
    # --- Post-processing: CONSOLIDATE PERIODS & GENERATE FINAL SETPOINTS ---
    logging.info("POST-PROCESSING START: Consolidating heating periods and generating setpoints.")
    
    # The remainder of the function handles consolidation,
    # setpoint generation, deduplication, truncation, and padding to 6 slots.

    for zone, daily_schedule in zone_schedule.items():
        for day, periods in daily_schedule.items():
            
            # 1. Consolidate all periods for the day
            consolidated_periods = _consolidate_periods(periods, eco_temp)
            
            # 2. Convert Consolidated Periods into a list of raw Setpoints (ON/OFF pairs)
            raw_setpoints: List[Dict[str, Union[str, float]]] = []
            
            # If the consolidated list is empty, just add a 00:00 ECO default
            if not consolidated_periods:
                raw_setpoints.append({"time": "00:00", "temp": eco_temp})
            
            for p in consolidated_periods:
                # Add Heat ON point
                raw_setpoints.append({"time": p["start"], "temp": p["temp"]})
                
                # Add ECO OFF point (only if the end time is not 00:00 or 23:59)
                if p["end"] != "23:59":
                    raw_setpoints.append({"time": p["end"], "temp": eco_temp})

            # --- 3. Final Compliance Steps (Deduplication, Padding, Sort) ---

            # A. Final De-Duplication (Remove redundant state changes)
            final_final_setpoints = []
            
            # First, sort the generated raw setpoints to handle points generated from the periods
            raw_setpoints.sort(key=lambda x: _time_string_to_minutes(x["time"]))
            
            for sp in raw_setpoints:
                if not final_final_setpoints:
                    final_final_setpoints.append(sp)
                elif sp["temp"] != final_final_setpoints[-1]["temp"]:
                    final_final_setpoints.append(sp)

            # B. Truncate to the maximum of 6 slots
            final_final_setpoints = final_final_setpoints[:6]


            # C. PADDING TO 6 SLOTS
            if len(final_final_setpoints) < 6:
                if not final_final_setpoints:
                    last_sp = {'time': '00:00', 'temp': eco_temp} 
                else:
                    last_sp = final_final_setpoints[-1]

                # Pad the list until it reaches length 6 by repeating the last setpoint
                while len(final_final_setpoints) < 6:
                    final_final_setpoints.append(last_sp)
                        
            # D. FINAL TIME SORTING (The ultimate safety check)
            final_final_setpoints.sort(key=lambda x: _time_string_to_minutes(x["time"]))
            logging.debug(f"OPTIMIZATION: Zone '{zone}' Day {day}: Final 6 setpoints sorted.")

            # Assign the guaranteed 6-point list back to the schedule structure
            zone_schedule[zone][day] = final_final_setpoints

    logging.info(f"AGGREGATION END: Successfully generated schedules for {len(zone_schedule)} NeoHub zones.")
    return zone_schedule

def _format_setpoints_for_neohub(
    daily_setpoints: List[Dict[str, Union[str, float]]]
) -> Dict[str, List[Union[str, float, int, bool]]]:
    """
    Takes the aggregated setpoints, pads them to exactly 6 levels, and formats them using the 
    correct NeoHub keys (wake, level1-level4, sleep) to resolve the empty profile issue.
    """
    # Fetch ECO_TEMPERATURE from global settings
    eco_temp = config["global_settings"].get("ECO_TEMPERATURE", 12.0) # Default if missing
    # 1. Prepare setpoints (Max 6)
    setpoints_to_use = daily_setpoints[:len(NEOHUB_SLOTS)] # Ensure max 6 are used
    
    # 2. Robust Padding: If less than 6, fill the remaining slots. (CRITICAL FIX)
    if setpoints_to_use:
        # Use the last valid setpoint (the final ECO time/temp) for padding.
        last_valid_sp = setpoints_to_use[-1] 
    else:
        # Failsafe: Default to 00:00 @ eco_temp if list is empty.
        last_valid_sp = {"time": "00:00", "temp": eco_temp}

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
            config["global_settings"].get("TEMPERATURE_SENSITIVITY", 10.0), # Default if missing
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
        
        # Check 3: Sequential Time Order and Data Integrity
        for slot_name in expected_slots:
            try:
                slot_data = daily_schedule[slot_name]
                
                # --- ENHANCEMENT 1: Check required list length (e.g., 4 elements) ---
                if len(slot_data) < 4:
                    return False, f"Data structure error on day '{day_name}', slot '{slot_name}': Expected 4 items (time, temp, etc.) but found {len(slot_data)}."
                
                time_str = slot_data[0]
                temp_value = slot_data[1]

                # --- ENHANCEMENT 2: Check Temperature Type ---
                if not isinstance(temp_value, (int, float)):
                    return False, f"Data type error on day '{day_name}', slot '{slot_name}': Temperature value '{temp_value}' is not a valid number."
                    
                current_time = datetime.datetime.strptime(time_str, "%H:%M").time()
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

def generate_neohub_reports(zone_statuses, hub_connectivity_status, zone_to_neohub_map):
    """
    Converts the raw zone_statuses and hub_connectivity_status into the final 
    structured report list (neohub_reports) for the JSON file.
    """
    neohub_reports = {}
    
    # 1. Group all zones by their NeoHub
    for zone_name, neohub_name in zone_to_neohub_map.items():
        if neohub_name not in neohub_reports:
            # Initialize the hub report entry
            neohub_reports[neohub_name] = {
                "name": neohub_name,
                "hub_status": hub_connectivity_status.get(neohub_name, "UNKNOWN"),
                "zones": [], # List to hold detailed zone reports
                "zones_updated": 0,
                "zones_total": 0,
                "message": "",
            }
        
        hub_report = neohub_reports[neohub_name]
        hub_report["zones_total"] += 1
        
        # 2. Extract and format the detailed zone status flags
        zone_data = zone_statuses.get(neohub_name, {}).get(zone_name, {})
        
        # Determine the overall zone status based on flags (e.g., if POST_PROFILE is 1)
        # We assume AGGREGATE_PROFILE=1 means the zone was processed successfully.
        is_updated = zone_data.get('AGGREGATE_PROFILE', 0) == 1
        
        if is_updated:
            hub_report["zones_updated"] += 1

        # Create the detailed zone report (This is what your HTML expects to display)
        detailed_zone_report = {
            "zone_name": zone_name,
            "status_flags": zone_data, # This is the dictionary: {'CREATE_PROFILE': 1, 'POST_PROFILE': 0, ...}
            "profile_status": "PROCESSED" if is_updated else "SKIPPED/FAILED"
        }
        
        hub_report["zones"].append(detailed_zone_report)

    # 3. Finalize messages and convert to a list
    final_reports_list = []
    for neohub_name, report in neohub_reports.items():
        if report["hub_status"] == "FAILED":
            report["hub_status"] = "CRITICAL_FAILURE"
            report["message"] = f"Connection and compatibility check FAILED."
        elif report["hub_status"] == "UNKNOWN":
            report["message"] = "No attempt made to connect during this run."
        elif not report["zones"]:
            report["hub_status"] = "SUCCESS" # Hub itself connected, but no zones were found to process.
            report["message"] = "No zones configured for this NeoHub."
        else:
            report["hub_status"] = "SUCCESS" # Overall hub status is good
            report["message"] = f"{report['zones_updated']} of {report['zones_total']} zones processed."
        
        final_reports_list.append(report)
        
    # 4. Handle hubs that have no zones mapped but are in the connectivity status (e.g., 'church_hall')
    # This prevents the report from losing status for failed hubs that have no zones in the current run's data.
    all_hubs = set(hub_connectivity_status.keys())
    hubs_in_report = set(r['name'] for r in final_reports_list)

    for missing_hub_name in all_hubs - hubs_in_report:
        status = hub_connectivity_status[missing_hub_name]
        if status == "FAILED":
             final_reports_list.append({
                "name": missing_hub_name,
                "hub_status": "CRITICAL_FAILURE",
                "zones": [],
                "zones_updated": 0,
                "zones_total": 0,
                "message": "Hub failed compatibility check and has no mapped zones."
            })

    return final_reports_list

async def check_ipc_flags():
    """Checks for IPC flag files created by the web GUI process."""
    if os.path.exists(CONFIG_RELOAD_FLAG):
        logging.debug("IPC: Config reload flag detected. Reloading configuration.")
        reload_config_from_disk()
        os.remove(CONFIG_RELOAD_FLAG) # Clear the flag
        # You may want to call update_heating_schedule() after a config reload

    if os.path.exists(MANUAL_RUN_FLAG):
        logging.debug("IPC: Manual run flag detected. Triggering immediate update.")
        # Ensure the manual run is executed in the event loop
        await update_heating_schedule()
        os.remove(MANUAL_RUN_FLAG) # Clear the flag

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
    profile_prefix: str,
    zone_statuses: dict,
    hub_connectivity_status: Dict[str, str] # <-- ADDED to the signature
) -> bool:
    """
    Applies a validated, aggregated weekly profile to a single NeoHub zone.
    It now incorporates hub status caching to prevent repeated connection checks,
    skipping straight to compliance if the hub status is 'PASSED'.
    """

    # --- 1. NEOHUB COMPATIBILITY CHECK (Now with Caching and PASSED bypass) ---
    hub_status = hub_connectivity_status.get(neohub_name)

    if hub_status == "PASSED":
        # Skip all checks and caching logic; proceed straight to compliance (2.)
        pass 
        
    elif hub_status == "FAILED":
        # Skip if already known to be FAILED
        logging.error(f"Skipping profile application for {zone_name}. Neohub {neohub_name} previously failed firmware/configuration checks.")
        return False
    
    # If status is UNKNOWN (None), run the compatibility check and cache the result
    else: # status is None (UNKNOWN)
        
        # Run the actual compatibility check
        is_compatible = await check_neohub_compatibility(neohub_object, neohub_name)
        
        # Cache the result for subsequent calls
        hub_connectivity_status[neohub_name] = "PASSED" if is_compatible else "FAILED"
        
        if not is_compatible:
            logging.error(f"Skipping profile application for {zone_name}. Neohub {neohub_name} failed firmware/configuration checks.")
            return False
    # --- END CACHING LOGIC ---

    # 2. --- PROFILE COMPLIANCE CHECK (Existing) ---
    is_compliant, reason = _validate_neohub_profile(profile_data, zone_name)
    if not is_compliant:
        logging.error(
            f"PROFILE COMPLIANCE FAILED for Zone '{zone_name}' ({profile_prefix}): {reason}. "
            f"Profile was NOT sent to NeoHub."
        )
        return False
    
    # --- NEW STATUS: Profile data is compliant and ready to send ---
    # CREATE_PROFILE is set to 1 here because the profile data has been fully created/formatted
    # and passed all application-level checks.
    try:
        zone_statuses[neohub_name][zone_name]['CREATE_PROFILE'] = 1 
        logging.debug(f"[{neohub_name}/{zone_name}] CREATE_PROFILE set to 1 (Compliant data).")
    except KeyError:
        # Defensive catch: Status should have been initialized in apply_aggregated_schedules
        logging.warning(f"Status not initialized for {neohub_name}/{zone_name}. Skipping status update.")
    # --- END NEW STATUS ---

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

        # --- NEW STATUS: Profile successfully posted to NeoHub ---
        zone_statuses[neohub_name][zone_name]['POST_PROFILE'] = 1
        logging.debug(f"[{neohub_name}/{zone_name}] POST_PROFILE set to 1 (Stored/Updated).")
        # --- END NEW STATUS ---

        logging.info(f"Profile successfully stored/updated. Preparing to activate ID {profile_id_to_activate} on '{zone_name}'.")

        # -----------------------------------------------------------
        # 5. --- CONDITIONAL ACTIVATION LOGIC (TWEAKED) ---
        # -----------------------------------------------------------
        # Check if the prefix CONTAINS "Current Week" (robust partial match)
        if "Current Week" in profile_prefix:
            # ONLY ACTIVATE Current Week profiles
            return await activate_profile_on_zones(neohub_name, profile_id_to_activate, zone_name, zone_statuses)
        else:
            # For Next Week (or any other prefix), simply confirm success without activating.
            zone_statuses[neohub_name][zone_name]['ACTIVATE_PROFILE'] = 1
            logging.info(f"Activation skipped for non-Current Week profile: '{profile_name}'.")
            return True

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

async def get_forecast_temperature(target_datetime: datetime.datetime) -> Optional[float]:
    """
    Fetches the predicted external temperature for a specific future datetime 
    from OpenWeatherMap.
    
    Returns the temperature in Celsius.
    """
    global OPENWEATHERMAP_API_KEY
    global OPENWEATHERMAP_CITY
    
    if not OPENWEATHERMAP_API_KEY:
        logging.warning("OPENWEATHERMAP_API_KEY is missing. Cannot fetch weather forecast.")
        return None

    try:
        WEATHER_FORECAST_API_URL = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            'q': OPENWEATHERMAP_CITY,
            'appid': OPENWEATHERMAP_API_KEY,
            'units': 'metric' # Request temperature in Celsius
        }
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, 
            lambda: requests.get(WEATHER_FORECAST_API_URL, params=params, timeout=10)
        )
        response.raise_for_status()
        
        data = response.json()
        
        # 1. Convert the target datetime to UTC timestamp for comparison
        target_ts = int(target_datetime.timestamp())

        # 2. Find the closest forecast point (OpenWeatherMap uses 3-hour intervals)
        closest_temp = None
        min_time_diff = float('inf')
        
        for item in data.get('list', []):
            forecast_ts = item.get('dt')
            if forecast_ts is not None:
                time_diff = abs(forecast_ts - target_ts)
                
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_temp = item.get('main', {}).get('temp')

        if closest_temp is not None:
            temp = float(closest_temp)
            logging.info(f"Forecast for {target_datetime.strftime('%Y-%m-%d %H:%M')}: {temp}°C (Diff: {min_time_diff // 60} min)")
            return temp
        
        logging.warning(f"No valid forecast data found for {OPENWEATHERMAP_CITY}.")
        return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather forecast for {OPENWEATHERMAP_CITY}: {e}")
        return None
    
def calculate_simulated_start_temp(
    event_id: int,
    zone_name: str,
    time_since_minutes: float,
    forecast_temp: float,
    hub_heat_loss_constant: float,
    zone_heat_loss_factor: float,
    global_config: Dict[str, Any]
) -> float:
    """
    Calculates the predicted internal start temperature (T_start) for a zone
    by simulating heat decay from T_target over the given time_since_minutes,
    dynamically adjusted for the external temperature forecast.
    
    The rate of decay is proportional to the temperature differential.
    """
    
    # 1. Fetch relevant config values with safe fallbacks
    global_settings = global_config.get("global_settings", {})
    T_target = global_settings.get("DEFAULT_TEMPERATURE", 18.0)
    T_eco = global_settings.get("ECO_TEMPERATURE", 12.0)
    
    # Safety guard for extremely long breaks
    if time_since_minutes >= 60 * 24 * 7:
        logging.info(f"Time since heat for {zone_name} exceeds 7 days. Assuming T_start = T_eco.")
        return T_eco
    
    # 2. Calculate Heat Loss Rate and Drop
    
    # Temperature differential driving the heat loss (T_target is the high-water mark)
    temp_differential = T_target - forecast_temp 
    
    # If forecast temp is higher than T_target, assume no decay below T_target.
    if temp_differential < 0:
        temp_differential = 0.0
    
    # Effective Thermal Mass: Hub constant * Zone factor. Higher value means slower decay.
    effective_thermal_mass = hub_heat_loss_constant * zone_heat_loss_factor
    
    if effective_thermal_mass <= 0:
        logging.error(f"Effective thermal mass for {zone_name} is non-positive. Using default T_eco.")
        return T_eco
        
    # Dynamic Decay Rate (Degrees lost per minute)
    decay_rate_per_min = temp_differential / effective_thermal_mass
    
    # Total Temp Drop over the elapsed time
    temp_drop = time_since_minutes * decay_rate_per_min
    
    # 3. Determine the Final Starting Temperature
    simulated_temp = T_target - temp_drop
    T_start = max(simulated_temp, T_eco)
    
    logging.debug(
        f"Decay for Event ID {event_id} in {zone_name}: T_target={T_target:.1f}, T_forecast={forecast_temp:.1f}. "
        f"Differential={temp_differential:.1f}°C. Time since: {time_since_minutes:.1f} min. "
        f"Rate: {decay_rate_per_min:.3f}°C/min. Total Drop: {temp_drop:.2f}°C. T_start: {T_start:.2f}°C."
        f"HLC: {hub_heat_loss_constant}, HLF: {zone_heat_loss_factor}."
    )
    
    return T_start

def calculate_preheat_duration(
    neohub_name: str, 
    zone_name: str,
    T_target: float, 
    T_start: float, 
    T_forecast: float
) -> float:
    """
    Calculates the required pre-heat duration in minutes, scaled by zone factor 
    and dynamically scaled for cold external temperatures.
    """
    global config
    global_settings = config.get("global_settings", {})
    zone_props = config.get("zones_properties", {}).get(zone_name, {}) # NEW: Fetch zone properties

    # 1. Required Temperature Rise
    T_rise_required = T_target - T_start
    if T_rise_required <= 0:
        return 0.0 
        
    # 2. Base Preheat Calculation (NOW MODIFIED)
    
    # Retrieve the global constant and the zone's heat loss factor
    ADJ_MIN_PER_DEGREE = global_settings.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 15.0)
    heat_loss_factor = zone_props.get("heat_loss_factor", 1.0) # NEW: Get zone factor
    
    # Base Preheat is now scaled by the zone's factor: a higher factor (like 3.0 for Nave) 
    # means it takes 3 times longer to heat up, aligning with its thermal mass.
    base_preheat = T_rise_required * ADJ_MIN_PER_DEGREE * heat_loss_factor 
    
    # 3. Dynamic Cold Weather Scaling (This part remains the same)
    MAX_MULTIPLIER = global_settings.get("HEAT_LOSS_MULTIPLIER_MAX", 2.0)
    
    if T_forecast < T_COLD_THRESHOLD:
        T_RANGE = T_COLD_THRESHOLD 
        
        coldness_ratio = (T_COLD_THRESHOLD - T_forecast) / T_RANGE
        coldness_ratio = max(0.0, min(1.0, coldness_ratio))
        
        multiplier = 1.0 + (coldness_ratio * (MAX_MULTIPLIER - 1.0))
        
        preheat_minutes = base_preheat * multiplier
        logging.debug(
            f"Cold scaling applied: T_forecast={T_forecast:.1f}, Multiplier={multiplier:.2f}. "
            f"Base preheat {base_preheat:.1f}m -> {preheat_minutes:.1f}m."
            f"HLF: {heat_loss_factor}, Zone: {zone_name})"
        )
    else:
        preheat_minutes = base_preheat
        logging.debug(
            f"No cold scaling applied."
            f"Base preheat {base_preheat:.1f}m -> {preheat_minutes:.1f}m."
            f"HLF: {heat_loss_factor}, Zone: {zone_name})"
        )

    return preheat_minutes

def calculate_cold_multiplier(forecast_temp: float, config: Dict[str, Any]) -> float:
    """Calculates the cold multiplier using configurable thresholds and maximums."""
    
    # Read values from the global_settings section of the config
    settings = config.get("global_settings", {})
    
    # Use .get with a default fallback (using the old hardcoded values) 
    # for safety, in case the new parameters are missing from the config file.
    T_WARM = settings.get("TEMP_WARM_THRESHOLD", 12.0)
    T_COLD = settings.get("TEMP_COLD_THRESHOLD", -5.0)
    HLM_MAX = settings.get("HEAT_LOSS_MULTIPLIER_MAX", 2.0)
    
    # The multiplier range (e.g., 1.0 to 1.46, so the range is 0.46)
    multiplier_range = HLM_MAX - 1.0 
    
    # Clamp the temperature within the range [T_COLD, T_WARM]
    clamped_temp = max(T_COLD, min(forecast_temp, T_WARM))
    
    # Calculate the ratio (0 at T_COLD, 1 at T_WARM)
    temp_range = T_WARM - T_COLD
    ratio = (clamped_temp - T_COLD) / temp_range
    
    # Scale the multiplier from HLM_MAX down to 1.0 (inverted ratio)
    # This is equivalent to: HLM_MAX - (ratio * multiplier_range)
    multiplier = HLM_MAX - (ratio * multiplier_range)
    
    return multiplier

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

async def calculate_decay_metrics_and_attach(
    booked_resources: List[Dict[str, Any]], 
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Calculates sequential temperature decay metrics (T_start, T_forecast, preheat_minutes, etc.)
    for each event in the booked_resources list and attaches the results to the event dictionary.
    """
    
    if not booked_resources:
        return []

    # Sort all events globally by start time. This is CRITICAL for sequential decay calculation.
    try:
        sorted_events = sorted(
            booked_resources, 
            key=lambda x: dateutil.parser.parse(x.get('starts_at')).astimezone(pytz.utc)
        )
    except Exception as e:
        logging.error(f"Failed to sort events by start time: {e}. Skipping decay calculation.")
        return booked_resources

    # --- Decay State Tracking Initialization ---
    # Key: (neohub_name, zone_name), Value: last heating end time (datetime object, UTC)
    last_end_times: Dict[Tuple[str, str], datetime.datetime] = {}
    
    # Get config constants
    T_DEFAULT = config["global_settings"].get("DEFAULT_TEMPERATURE", 18.0)
    MIN_PREHEAT = config["global_settings"].get("PREHEAT_TIME_MINUTES", 30.0)
    MINUTES_PER_DEGREE = config["global_settings"].get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 15.0)
    
    # Initialize the default last end time far in the past (e.g., system start)
    DEFAULT_LAST_END = datetime.datetime.min.replace(tzinfo=pytz.utc)
    
    # Get the global mapping from location name to config settings
    location_configs = config.get('locations', {})

    # 2. Sequential Calculation Loop
    for event in sorted_events:
        location_name = event.get('location_name')
        event_start_iso = event.get('starts_at') 
        event_end_iso = event.get('ends_at')

        location_config = location_configs.get(location_name)
        if not location_config:
            continue
            
        neohub_name = location_config['neohub']
        #--- Fetch Hub Heat Loss Constant ---
        hub_settings = config.get('hub_settings', {}).get(neohub_name, {})
        HUB_HLC = hub_settings.get("HEAT_LOSS_CONSTANT", 3000.0)
        #--- Fetch Zone Heat Loss Factor for the PRIMARY zone ---
        zones_list = location_config.get('zones', []) # e.g., ['Main Church', 'Raphael Room corridor and toilets']

        if zones_list:
            primary_zone_name = zones_list[0] # Takes 'Main Church'
            
            # Looks into {"zone_properties": {"Main Church": {"heat_loss_factor": 4.5}}}
            zone_properties = config.get('zone_properties', {}).get(primary_zone_name, {})
            ZONE_HLF_FOR_DECAY = zone_properties.get("heat_loss_factor", 1.0) 
        else:
            ZONE_HLF_FOR_DECAY = 1.0

        try:
            start_dt = dateutil.parser.parse(event_start_iso).astimezone(pytz.utc)
            end_dt = dateutil.parser.parse(event_end_iso).astimezone(pytz.utc)
        except Exception:
            logging.error(f"Failed to parse time for event at '{location_name}'. Skipping decay calculation.")
            continue
            
        # Pre-fetch the weather forecast once per event
        forecast_temp = await get_forecast_temperature(start_dt)
        if forecast_temp is None:
            logging.warning(f"Could not get forecast for {location_name} at {start_dt}. Using fallback 0.0°C.")
            forecast_temp = 0.0
        # ⚠️ NEW STEP: Calculate the dynamic cold multiplier
        COLD_MULTIPLIER = calculate_cold_multiplier(forecast_temp, config)

        # Now, apply to all affected zones and update decay state
        for zone_name in location_config['zones']:
            
            # Use the unique tuple key for decay state tracking
            decay_tracking_key = (neohub_name, zone_name)

            # --- A. Determine Time Since Last Heat ---
            last_end_time = last_end_times.get(decay_tracking_key, DEFAULT_LAST_END)
            
            if start_dt <= last_end_time:
                time_since_minutes = 0.0
            else:
                time_since = start_dt - last_end_time
                time_since_minutes = time_since.total_seconds() / 60

            # --- B. Calculate Simulated Start Temp (T_start) ---
            T_start = calculate_simulated_start_temp(
                event_id=event.get('id'),
                zone_name=zone_name,
                time_since_minutes=time_since_minutes,
                forecast_temp=forecast_temp,
                hub_heat_loss_constant=HUB_HLC,
                zone_heat_loss_factor=ZONE_HLF_FOR_DECAY,
                global_config=config
            )
            
            # --- C. Calculate Preheat Time (T_required_rise and final_preheat_minutes) ---
            
            # FIX: Retrieve location-specific heat_loss_factor, default to 3.0
            HEAT_LOSS_FACTOR = location_config.get("heat_loss_factor", 3.0) 
            
            T_required_rise = T_DEFAULT - T_start
            
            # Calculate dynamic preheat
            # Dynamic Preheat = Rise * MPD * HLF * Cold_Multiplier
            dynamic_preheat_minutes = max(
                0.0, 
                T_required_rise * MINUTES_PER_DEGREE * HEAT_LOSS_FACTOR * COLD_MULTIPLIER
            )
            # Apply the safety minimum preheat time
            final_preheat_minutes = max(dynamic_preheat_minutes, MIN_PREHEAT)

            # --- D. Attach results and Update Decay State ---
            # IMPORTANT: Attach metrics using the simple string key (zone_name)
            event.setdefault('decay_metrics', {})
            event['decay_metrics'][zone_name] = { 
                "time_since_minutes": round(time_since_minutes, 1),
                "T_forecast": round(forecast_temp, 1),
                "T_start_simulated": round(T_start, 1),
                "cold_multiplier": round(COLD_MULTIPLIER, 2),
                "preheat_minutes": round(final_preheat_minutes, 1) # CRITICAL
            }
            
            logging.debug(f"Preheat for {zone_name} in event {event.get('id')}: T_start={T_start:.1f}C, Rise={T_required_rise:.1f}C, Preheat={final_preheat_minutes:.1f}min, HLF Used={HEAT_LOSS_FACTOR}, Cold Mult={COLD_MULTIPLIER:.2f}")
            
            # Update the last heat end time for this zone for the NEXT event.
            last_end_times[decay_tracking_key] = end_dt

    return sorted_events

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
    zone_to_neohub_map: Dict[str, str],
    zone_statuses: dict,
    hub_connectivity_status: Dict[str, str] # Keep this in the signature, but only pass it along
) -> None:
    """
    Takes aggregated weekly setpoints, formats them for the NeoHub, validates them,
    and applies them to the corresponding NeoHub zones. The hub compatibility check
    has been moved to apply_single_zone_profile for isolation.
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
        # neohub_object will be None if the object hasn't been created yet (which is fine now)
        neohub_object = hubs.get(neohub_name) 

        if not neohub_object and neohub_name not in hub_connectivity_status:
            # We don't check for object presence here anymore, as the check happens in the sub-function.
            # We only check if the neohub name is known to be mapped.
            if not neohub_name:
                logging.error(f"Cannot apply schedule for zone '{zone_name}': Zone name not mapped to a Neohub. Skipping.")
                continue

        # --- Initialize Status and Mark AGGREGATE_PROFILE Success ---
        # 1. Initialize the status dictionary for the current zone (if it wasn't pre-initialized)
        zone_statuses.setdefault(neohub_name, {}).setdefault(zone_name, {
            'CREATE_PROFILE': 0, 
            'AGGREGATE_PROFILE': 0,
            'POST_PROFILE': 0, 
            'ACTIVATE_PROFILE': 0
        })
        
        # 2. Mark successful aggregation (this function's job)
        zone_statuses[neohub_name][zone_name]['AGGREGATE_PROFILE'] = 1 
        logging.debug(f"[{neohub_name}/{zone_name}] AGGREGATE_PROFILE set to 1.")
        # --- END NEW STATUS LOGIC ---

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
                profile_prefix,
                zone_statuses,
                hub_connectivity_status # <-- NEW: Pass cache for compatibility check
            )
        )

# CRITICAL FIX: Await all gathered tasks before exiting the function
    if tasks:
        logging.info(f"Applying {len(tasks)} zone profiles for {profile_prefix} in parallel.")
        await asyncio.gather(*tasks) # <--- THIS IS THE FIX
    else:
        logging.info(f"No profiles to apply for {profile_prefix}.")

async def activate_profile_on_zones(neohub_name: str, profile_id: int, zone_name: str, zone_statuses: dict) -> bool:
    """
    Activates a specific profile ID on one or more heating zones using the 
    RUN_PROFILE_ID command, with robust response handling.
    """
    logging.info(f"Attempting to activate Profile ID {profile_id} on zone(s): '{zone_name}' via {neohub_name}.")

    try:
        inner_command = {
            "RUN_PROFILE_ID": [
                profile_id,
                zone_name
            ]
        }
        
        response: Optional[Dict[str, Any]] = await send_command(neohub_name, inner_command)

        # ----------------------------------------------------------------------
        # Convert SimpleNamespace to dict for reliable access
        # ----------------------------------------------------------------------
        if response and not isinstance(response, dict):
            try:
                # Use vars() to convert SimpleNamespace to a dictionary
                response = vars(response)
            except TypeError:
                # Log the issue but proceed to check the result
                logging.warning(f"Response for RUN_PROFILE_ID was neither a dict nor convertible to one for '{zone_name}'.")
        # ----------------------------------------------------------------------

        # Now check the 'result' key, which works reliably on the dictionary (or None)
        if response and response.get('result') == 'profile was run':
            logging.info(f"Successfully activated Profile ID {profile_id} on zone(s) '{zone_name}'. Hub result: '{response.get('result')}'")
            
            # --- NEW STATUS LOGIC: Mark Activation Success ---
            try:
                zone_statuses[neohub_name][zone_name]['ACTIVATE_PROFILE'] = 1 
                logging.debug(f"[{neohub_name}/{zone_name}] ACTIVATE_PROFILE set to 1 (Success).")
            except KeyError:
                logging.warning(f"Status structure missing for {neohub_name}/{zone_name} during activation update.")
            # --- END NEW STATUS LOGIC ---
            
            return True
        else:
            logging.error(f"Failed to activate profile {profile_id} on '{zone_name}'. Hub response: {response}")
            return False

    except Exception as e:
        # The SimpleNamespace error should be caught here if conversion failed, 
        # but the conversion logic should prevent it from happening.
        logging.error(f"Error during RUN_PROFILE_ID command for zone '{zone_name}' on {neohub_name}: {e}", exc_info=True)
        return False

async def update_heating_schedule() -> None:
    """
    Updates the heating schedule based on upcoming bookings,
    aggregating schedules by neohub and zone using a rolling 7-day window.
    Now includes hub connection caching and guarantees status reporting on exit.
    """
    # --- START CRITICAL LIFECYCLE MANAGEMENT ---
    
    # 1. Reset the high-ID application counter (NEW LINES)
    global _command_id_counter
    _command_id_counter.reset()

    logging.info("--- STARTING HEATING SCHEDULE UPDATE PROCESS (7-Day Rolling Window) ---")
    global config
    
    # NEW: Multi-stage status tracking dictionary defined with the four stages.
    # Status: 0 (Initial/Failure), 1 (Success)
    zone_statuses = {} 
    
    # NEW: Dictionary to cache compatibility check results for NeoHubs in this run.
    # Key: neohub_name (str), Value: "PASSED" or "FAILED" (str)
    hub_connectivity_status: Dict[str, str] = {}
    
    neohub_reports = []
    # Initialize overall_status to FAILURE as the default.
    overall_status = "FAILURE" 
    location_timezone = None # Initialize
    
    # --- CORE PROCESSING LOGIC (Wrapped in try/except for critical failure handling) ---
    try:

        # 1. Configuration Validation
        if config is None:
            logging.error("Configuration not loaded. Exiting.")
            overall_status = "CRITICAL_FAILURE"
            raise Exception("Configuration not loaded.") 
            
        if not validate_config(config):
            logging.error("Invalid configuration. Exiting.")
            overall_status = "CRITICAL_FAILURE"
            raise Exception("Invalid configuration.")
        
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
        if not data:
            overall_status = "PARTIAL_FAILURE"
            raise StopIteration("No data received from ChurchSuite.")

        booked_resources = data.get("booked_resources", [])
        resources = data.get("resources", [])
            
        logging.info(f"Fetched {len(booked_resources)} total bookings and {len(resources)} resources from ChurchSuite.")

        if not booked_resources:
            overall_status = "SUCCESS" # Successfully confirmed no actions needed
            raise StopIteration("No bookings to process.")

        if not resources:
            overall_status = "FAILURE" # Fail if bookings exist but we can't map them
            raise Exception("No resources found.")

        # Create a map to resolve resource_id to location name
        resource_id_to_name = {r.get('id'): r.get('name') for r in resources if r.get('id') and r.get('name')}
        logging.debug(f"RESOURCE MAP: Created ID to Name map with {len(resource_id_to_name)} entries.")
        
        logging.info("Attaching location names to booked resources for configuration lookup.")
        processed_bookings = []
        for booking in booked_resources:
            resource_id = booking.get("resource_id")
            # CRITICAL: Attach the name derived from the resource map
            if resource_id in resource_id_to_name:
                location_name = resource_id_to_name[resource_id]
                booking['location_name'] = location_name 
                # Optional: Add a check that the location name is configured
                if location_name not in config.get('locations', {}):
                    logging.warning(f"Booking location '{location_name}' is not found in config['locations']. Skipping decay calculation.")
                    continue
                processed_bookings.append(booking)
            else:
                logging.warning(f"Booking ID {booking.get('id')} has unknown resource ID {resource_id}. Skipping decay calculation.")

        booked_resources = processed_bookings # Use the new, cleaned list for the next step

        # NEW STEP: Calculate Decay States and Attach Metrics
        logging.info("Calculating sequential temperature decay states and fetching per-event forecasts...")
        booked_resources = await calculate_decay_metrics_and_attach(booked_resources, config)
        logging.info("Decay calculation complete.")            
        
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

        # 3. Get External Temperature (REMOVED: Now handled per-event in decay calculation)
        external_temperature = 0.0 # Placeholder value for legacy function signature compatibility
        logging.info("External temperature fetch skipped; using per-event forecasts for decay calculation.")

        # 4. AGGREGATE SCHEDULES BY ZONE (The Critical Step)
        aggregated_p1_schedules = create_aggregated_schedule(
            profile_1_bookings, 
            external_temperature, 
            config,
            resource_id_to_name,
            zone_statuses
        )
        logging.info(f"AGGREGATION RESULT (Profile 1): {len(aggregated_p1_schedules)} final locations/zones scheduled.")
            
        aggregated_p2_schedules = create_aggregated_schedule(
            profile_2_bookings, 
            external_temperature, 
            config,
            resource_id_to_name,
            zone_statuses
        )
        logging.info(f"AGGREGATION RESULT (Profile 2): {len(aggregated_p2_schedules)} final locations/zones scheduled.")

        # 5. APPLY AGGREGATED SCHEDULES
        # The profile names remain "Current Week" and "Next Week" for the NeoHub hardware
        # NEW: Pass hub_connectivity_status cache
        await apply_aggregated_schedules(
            aggregated_p1_schedules, "Current Week", config, zone_to_neohub_map, zone_statuses, hub_connectivity_status
        )
        await apply_aggregated_schedules(
            aggregated_p2_schedules, "Next Week", config, zone_to_neohub_map, zone_statuses, hub_connectivity_status
        )
        
        # If we reached this point successfully, default overall_status to SUCCESS
        overall_status = "SUCCESS"

    except StopIteration as e:
        # Expected exit path for 'No bookings' or 'No data'. Status is set above.
        logging.info(f"Graceful exit: {e}")
        
    except Exception as e:
        # CRITICAL UNHANDLED ERROR
        logging.critical(f"UNHANDLED CRITICAL ERROR during update_heating_schedule: {e}")
        overall_status = "CRITICAL_FAILURE"

    finally:
        # --- 6. FINAL STATUS COMPILATION AND SAVING (Inside finally block for guaranteed run) ---
        
        # Only analyze the results if processing went through and wasn't immediately halted 
        # (i.e., not a critical failure before config or setup)
        if overall_status != "CRITICAL_FAILURE" and config is not None: 
        
            all_neohub_names = list(config["neohubs"].keys())
            total_zones_processed = 0
            successful_zones = 0
            neohub_reports = [] # Reset/ensure reports are fresh
            
            # --- Check Hub Connectivity Status and Individual Zone Status ---
            for neohub_name in all_neohub_names:
                conn_status = hub_connectivity_status.get(neohub_name)
                zones_configured = config["neohubs"][neohub_name].get("zones", [])
                
                if conn_status == "FAILED":
                    # Connection failure is a critical failure for this hub
                    # The zones were skipped, so we report the hub failure immediately.
                    neohub_reports.append({
                        "name": neohub_name,
                        "hub_status": "CRITICAL_FAILURE",
                        "zones_updated": 0,
                        "zones_total": len(zones_configured),
                        "message": "Connection and compatibility check FAILED."
                    })
                    logging.error(f"Hub '{neohub_name}' permanently failed connection check. All its zones were skipped.")
                    # Skip to next hub, do not check individual zone success below
                    continue
                
                # --- Check individual zone status for PASSED hubs ---
                zone_results = zone_statuses.get(neohub_name, {})
                
                hub_total_zones = len(zones_configured)
                hub_success_count = 0
                
                for zone_name in zones_configured:
                    total_zones_processed += 1
                    
                    # Check for the final action flags (ACTIVATE/POST) which signal success
                    # A zone is successful if the final desired action (ACTIVATE or POST) was set to 1.
                    # This implicitly accounts for both Current Week (ACTIVATE=1) and Next Week (POST=1 and ACTIVATE=0/skipped)
                    post_status = zone_results.get(zone_name, {}).get('POST_PROFILE', 0)
                    
                    # We assume POST_PROFILE=1 means the profile was successfully created/updated 
                    # and the system proceeded to the activation step (which might be skipped for Next Week).
                    if post_status == 1: 
                        successful_zones += 1
                        hub_success_count += 1
                    
                # Determine hub-level status
                if hub_total_zones == 0:
                    hub_status = "SUCCESS" 
                    message = "No zones configured for this NeoHub."
                elif hub_success_count == hub_total_zones:
                    hub_status = "SUCCESS"
                    message = f"Updated {hub_success_count} of {hub_total_zones} zones successfully."
                elif hub_success_count > 0:
                    hub_status = "PARTIAL_FAILURE"
                    message = f"Partial update: {hub_success_count} of {hub_total_zones} zones updated."
                else:
                    # This block executes when hub_success_count is 0 (all zones failed to update/communicate).
                    
                    # NEW LOGIC: Check if any zone successfully aggregated to upgrade status.
                    any_zone_aggregated = False
                    
                    # zone_results is the dictionary containing results for all zones on the current hub
                    for zone_name in zone_results:
                        # AGGREGATE_PROFILE is set to 1 if the schedule logic completed successfully.
                        if zone_results.get(zone_name, {}).get('AGGREGATE_PROFILE', 0) == 1:
                            any_zone_aggregated = True
                            break
                            
                    if any_zone_aggregated:
                        # Schedule logic worked, but communication/hub failed for all zones (Yellow status).
                        hub_status = "PARTIAL_FAILURE" 
                        message = f"Partial failure: Schedule aggregation succeeded, but profile update failed for all {hub_total_zones} zones. This often indicates a communication error or hub crash."
                    else:
                        # True failure (aggregation or basic configuration check failed for all zones) (Red status).
                        hub_status = "FAILURE"
                        message = f"Failed to update all {hub_total_zones} zones due to profile errors or logic failure."

                neohub_reports.append({
                    "name": neohub_name,
                    "hub_status": hub_status,
                    "zones_updated": hub_success_count,
                    "zones_total": hub_total_zones,
                    "message": message
                })


            # --- Determine Overall System Status ---
            if total_zones_processed > 0:
                # Count critical hub failures
                critical_hub_failures = sum(1 for report in neohub_reports if report['hub_status'] == 'CRITICAL_FAILURE')
                
                if critical_hub_failures > 0:
                    overall_status = "CRITICAL_FAILURE"
                elif successful_zones == total_zones_processed:
                    overall_status = "SUCCESS"
                elif successful_zones > 0:
                    overall_status = "PARTIAL_FAILURE"
                else:
                    overall_status = "FAILURE" 
            elif overall_status == "FAILURE":
                # Only reachable if config/bookings failed, but we still ensure a report
                pass
            else:
                overall_status = "SUCCESS" 
                
        # Ensure a report exists for critical failure
        if overall_status == "CRITICAL_FAILURE" and not neohub_reports:
            neohub_reports.append({
                "name": "N/A",
                "hub_status": overall_status,
                "zones_updated": 0,
                "zones_total": 0,
                "message": "A critical error occurred before processing could be fully initialized."
            })

        if location_timezone is None:
            location_timezone = pytz.timezone("UTC")
            
        last_run_time = datetime.datetime.now(location_timezone).strftime("%Y-%m-%d %H:%M:%S %Z")
        
        neohub_reports = generate_neohub_reports(
            zone_statuses, 
            hub_connectivity_status, 
            zone_to_neohub_map
        )

        status_data = {
            "last_run_time": last_run_time,
            "overall_status": overall_status,
            "neohub_reports": neohub_reports
        }
        
        # Assume write_status_file(status_data) exists and is the one that writes SCHEDULER_STATUS_FILE
        write_status_file(status_data)
        
        logging.info(f"--- HEATING SCHEDULE UPDATE PROCESS COMPLETE with status: {overall_status} ---")

async def run_scheduler_forever(scheduler):
    """Starts the scheduler and keeps the asyncio loop running indefinitely."""
    try:
        scheduler.start()
        # Keep the main thread alive indefinitely until a stop signal is received
        while True:
            await asyncio.sleep(600) # Use async sleep to not block the event loop
    except (KeyboardInterrupt, SystemExit):
        logging.info("Received shutdown signal.")
    finally:
        logging.info("Shutting down scheduler...")
        scheduler.shutdown()
        logging.info("Closing Neohub connections...")
        # Add the connection closing logic here, if not handled elsewhere
        # global _neohub_clients
        # for client in _neohub_clients.values():
        #     await client.close_connection()
        logging.info("Exiting...")

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
    
    # Define all required global settings and their defaults (as floats)
    DEFAULT_GLOBAL_SETTINGS = {
        "PREHEAT_TIME_MINUTES": 30.0,
        "DEFAULT_TEMPERATURE": 18.0,
        "ECO_TEMPERATURE": 12.0,
        "TEMPERATURE_SENSITIVITY": 10.0,
        "PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE": 15.0,
        "HEAT_LOSS_MULTIPLIER_MAX": 1.46             # New float default for cold weather scaling
    }
    
    # Ensure the 'global_settings' key exists
    if "global_settings" not in config:
        config["global_settings"] = {}
        
    for key, default_value in DEFAULT_GLOBAL_SETTINGS.items():
        current_value = config["global_settings"].get(key)
        
        # Check if value is missing or needs type casting
        if current_value is None:
            config["global_settings"][key] = default_value
        else:
            try:
                # Ensure all global settings are floats
                config["global_settings"][key] = float(current_value)
            except (ValueError, TypeError):
                config["global_settings"][key] = default_value
                logging.warning(f"Invalid float value for global setting '{key}'. Using default: {default_value}")
    
    # 2. Ensure 'hubs_settings' exists and has defaults for known hubs
    if "hub_settings" not in config:
        config["hub_settings"] = {}
        
    # Iterate over all defined NeoHubs to ensure they have a HEAT_LOSS_CONSTANT
    for neohub_name in config.get("neohubs", {}).keys():
        if neohub_name not in config["hub_settings"]:
            config["hub_settings"][neohub_name] = {}
        
        hub_settings = config["hub_settings"][neohub_name]
        
        # Apply default for HEAT_LOSS_CONSTANT (Building-specific thermal inertia)
        h_l_c = hub_settings.get("HEAT_LOSS_CONSTANT")
        if h_l_c is None:
            hub_settings["HEAT_LOSS_CONSTANT"] = 3000.0
            logging.info(f"Applied default HEAT_LOSS_CONSTANT to hub '{neohub_name}': 3000.0")
        else:
            try:
                # Ensure value is float
                hub_settings["HEAT_LOSS_CONSTANT"] = float(h_l_c)
            except (ValueError, TypeError):
                 hub_settings["HEAT_LOSS_CONSTANT"] = 3000.0
                 logging.warning(f"Invalid float value for HEAT_LOSS_CONSTANT on hub '{neohub_name}'. Using default: 3000.0")
            
    # 3. Ensure 'zones_properties' exists (content will be validated elsewhere/manually added)
    if "zones_properties" not in config:
        config["zones_properties"] = {}

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
    # --- SCHEDULER SETUP ---
    scheduler = AsyncIOScheduler()

    # Run update_heating_schedule() immediately.
    # We run this sync, but it's crucial to only do this once.
    try:
        asyncio.run(update_heating_schedule())
    except RuntimeError as e:
        # Handle case where event loop might already be running (rare in this context, but safe)
        logging.error(f"Error during immediate heating schedule update: {e}")

    # Schedule the recurring jobs
    scheduler.add_job(update_heating_schedule, "interval", minutes=60)
    scheduler.add_job(
        check_ipc_flags, 
        'interval', 
        seconds=5, # Check every 5 seconds for a new request
        id='ipc_monitor_job', 
        name='IPC Flag Monitor'
    )
    
    # --- START THE ASYNC LOOP ---
    logging.info("Starting AsyncIOScheduler and background loop...")
    asyncio.run(run_scheduler_forever(scheduler))

if __name__ == "__main__":
    main()