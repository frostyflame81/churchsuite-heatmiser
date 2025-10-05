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
# FIX: Ensure temperatures are treated as floats as required by the NeoHub API
DEFAULT_TEMPERATURE = float(os.environ.get("DEFAULT_TEMPERATURE", 19.0))
ECO_TEMPERATURE = float(os.environ.get("ECO_TEMPERATURE", 12.0))
TEMPERATURE_SENSITIVITY = int(os.environ.get("TEMPERATURE_SENSITIVITY", 10))
PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE = float(
    os.environ.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 5)
)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO").upper()

# Set up logging
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Global dictionary to hold NeoHub connections
neohubs: Dict[str, NeoHub] = {}

def load_config():
    """Load configuration from a JSON file."""
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        logging.error(f"Config file not found: {CONFIG_FILE}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from config file: {CONFIG_FILE}")
        return None

def connect_to_neohub(neohub_name, neohub_config):
    """Initializes and connects to the NeoHub."""
    host = neohub_config.get("host")
    token = neohub_config.get("token")
    if not host or not token:
        logging.error(f"Missing host or token for Neohub: {neohub_name}")
        return False
    
    # Initialize the NeoHub object
    neohubs[neohub_name] = NeoHub(host=host, token=token, request_timeout=15, request_attempts=1)
    
    # Connection is handled lazily by the send_command wrapper.
    logging.info(f"NeoHub instance created for {neohub_name} at {host}")
    return True

async def send_command(neohub_name: str, command: Any) -> Optional[Dict[str, Any]]:
    """A wrapper for NeoHub.send_command with error handling."""
    hub = neohubs.get(neohub_name)
    if not hub:
        logging.error(f"NeoHub instance not found for {neohub_name}")
        return None
    
    try:
        # Check if the hub is running, if not, try to start it
        if not hub.running:
            await hub.start()
        
        # NOTE: The command is expected to be a valid JSON string for STORE_PROFILE2
        # to bypass the library's incorrect internal dictionary serialization.
        if isinstance(command, dict):
             logging.warning("send_command received a dict, attempting to pass to library. This may fail for complex commands.")
        
        response = await hub.send_command(command)
        return response
    except NeoHubConnectionError as e:
        logging.error(f"Connection error with {neohub_name}: {e}")
    except NeoHubUsageError as e:
        logging.error(f"Usage error with {neohub_name}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred with {neohub_name}: {e}")
    finally:
        # Optional: Disconnect after command to prevent long-running sockets
        # await hub.disconnect()
        pass
    return None

async def get_zones(neohub_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve zone names and IDs."""
    logging.info(f"Retrieving zones from Neohub {neohub_name}")
    command = {"GET_ZONES": 0}
    response = await send_command(neohub_name, command)
    return response

async def get_weather_data(city: str) -> Optional[float]:
    """Fetch current outdoor temperature from OpenWeatherMap."""
    if not OPENWEATHERMAP_API_KEY:
        logging.warning("OPENWEATHERMAP_API_KEY is not set. Skipping weather data fetch.")
        return None

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        current_temp = data["main"]["temp"]
        logging.info(f"Current outdoor temperature in {city}: {current_temp}°C")
        return float(current_temp)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data for {city}: {e}")
        return None

async def get_churchsuite_events() -> List[Dict[str, Any]]:
    """Fetch ChurchSuite events."""
    if not CHURCHSUITE_URL:
        logging.warning("CHURCHSUITE_URL is not set. Skipping ChurchSuite event fetch.")
        return []

    try:
        response = requests.get(CHURCHSUITE_URL, timeout=10)
        response.raise_for_status()
        events = response.json()
        logging.info(f"Successfully fetched {len(events)} ChurchSuite events.")
        return events
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching ChurchSuite events: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding ChurchSuite JSON: {e}")
        return []

def get_preheat_minutes(outdoor_temp: Optional[float], target_temp: float, current_indoor_temp: Optional[float]) -> int:
    """Calculate preheat time based on indoor/outdoor temperature difference."""
    
    # Fallback to a default time if no complex calculation can be done
    if outdoor_temp is None and current_indoor_temp is None:
        logging.info(f"Using default preheat time: {PREHEAT_TIME_MINUTES} minutes.")
        return PREHEAT_TIME_MINUTES

    # 1. Determine the effective starting temperature (use cooler of indoor/outdoor)
    start_temp = current_indoor_temp if current_indoor_temp is not None else target_temp 
    if outdoor_temp is not None and outdoor_temp < start_temp:
        start_temp = outdoor_temp

    # 2. Calculate the required temperature rise
    temp_diff = target_temp - start_temp
    
    if temp_diff <= 0:
        return 0  # No preheating needed if already at or above target

    # 3. Calculate adjustment based on sensitivity (minutes per degree)
    adjustment = temp_diff * PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE
    
    # 4. Apply the minimum preheat time
    preheat_minutes = int(max(PREHEAT_TIME_MINUTES, adjustment))
    
    logging.info(f"Target Rise: {temp_diff:.1f}°C. Calculated preheat adjustment: {adjustment:.0f}m. Final preheat time: {preheat_minutes}m.")
    return preheat_minutes

def calculate_schedule(
    neohub_name: str,
    zone_config: Dict[str, Any],
    churchsuite_data: List[Dict[str, Any]],
    zones_live_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Generates the heating profile data for a single zone based on events."""
    
    local_timezone = pytz.timezone(zone_config.get("timezone", "UTC"))
    
    # Get current indoor temperature for pre-heat calculation
    zone_name = zone_config["zone_name"]
    current_indoor_temp = None
    if zone_name in zones_live_data and zones_live_data[zone_name].get("temp") is not None:
         # Temperature is expected to be a float from the live data
        current_indoor_temp = float(zones_live_data[zone_name]["temp"])
    
    # Get outdoor temperature
    outdoor_temp = zones_live_data.get("outdoor_temp")
    
    schedule_data: Dict[str, Any] = {}
    
    # --- 1. Filter and Process Events ---
    
    # Get relevant events for the current zone and time frame (Next 7 days)
    now = datetime.datetime.now(local_timezone)
    seven_days_later = now + datetime.timedelta(days=7)

    relevant_events = [
        event for event in churchsuite_data
        if zone_config["zone_name"] in event.get("rooms", []) and 
           dateutil.parser.parse(event["end_date"]).replace(tzinfo=local_timezone) > now and
           dateutil.parser.parse(event["start_date"]).replace(tzinfo=local_timezone) < seven_days_later
    ]

    # Group events by day of the week (0=Monday, 6=Sunday)
    daily_events: Dict[int, List[Dict[str, Any]]] = {i: [] for i in range(7)}
    for event in relevant_events:
        start_date = dateutil.parser.parse(event["start_date"]).replace(tzinfo=local_timezone)
        # Check day of week for start date
        daily_events[start_date.weekday()].append(event)
    
    # --- 2. Iterate through each day (Monday 0 to Sunday 6) ---

    for day_index in range(7):
        day_name = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][day_index]
        events_on_day = daily_events.get(day_index, [])
        events_on_day.sort(key=lambda x: dateutil.parser.parse(x["start_date"]))
        
        # This list will hold (level_name, datetime_object, temperature) for all 6 slots
        day_schedule_slots: List[tuple[str, datetime.datetime, float]] = []

        # Find the first event (Wake time logic)
        first_event_start = None
        if events_on_day:
            first_event_start = dateutil.parser.parse(events_on_day[0]["start_date"]).replace(tzinfo=local_timezone)
        
        # Determine the target time and temperature for the first heat event
        if first_event_start:
            # Calculate required pre-heat time
            preheat_minutes = get_preheat_minutes(outdoor_temp, DEFAULT_TEMPERATURE, current_indoor_temp)
            
            # Calculate actual heating start time
            heating_start_time = first_event_start - datetime.timedelta(minutes=preheat_minutes)
            
            # Add WAKE
            day_schedule_slots.append(("wake", heating_start_time, DEFAULT_TEMPERATURE))

            # Add subsequent events (Level 1-4, Sleep) based on events.
            # For simplicity and to avoid complexity, we'll map the next 4 event start times
            # and the final sleep time, or use default filler times if needed.
            
            # Use the actual event start times for the next levels if available
            event_index = 1
            for event in events_on_day[1:]:
                 if event_index <= 4: # Map up to level4
                     start_time = dateutil.parser.parse(event["start_date"]).replace(tzinfo=local_timezone)
                     day_schedule_slots.append((f"level{event_index}", start_time, DEFAULT_TEMPERATURE))
                     event_index += 1

            # Determine the SLEEP time (e.g., 30 minutes after last event ends, or default 23:00)
            last_event_end = first_event_start + datetime.timedelta(hours=1) # Default end time
            if events_on_day:
                 last_event_end = dateutil.parser.parse(events_on_day[-1]["end_date"]).replace(tzinfo=local_timezone)
            
            sleep_time = last_event_end + datetime.timedelta(minutes=30)
            
            # If sleep time is beyond midnight, cap it at 23:30 for the current day
            if sleep_time.date() > first_event_start.date() or sleep_time.hour > 23:
                 sleep_time = first_event_start.replace(hour=23, minute=30, second=0, microsecond=0)
            
            # Add SLEEP
            day_schedule_slots.append(("sleep", sleep_time, ECO_TEMPERATURE))

        else:
            # Default ECO schedule for days with no events
            # WAKE (08:00) at DEFAULT_TEMP, SLEEP (23:00) at ECO_TEMP.
            day_schedule_slots.append(("wake", now.replace(hour=8, minute=0, second=0, microsecond=0), DEFAULT_TEMPERATURE))
            day_schedule_slots.append(("sleep", now.replace(hour=23, minute=0, second=0, microsecond=0), ECO_TEMPERATURE))
        
        
        # --- 3. Fill remaining slots and Enforce Chronology ---
        
        # Ensure we have exactly 6 levels (must match the API structure: wake, level1, level2, level3, level4, sleep)
        
        # Map the current slots to a temporary structure for easy lookup
        temp_slot_map = {name: (dt, temp) for name, dt, temp in day_schedule_slots}
        
        # Define the 6 keys in the required API order
        required_levels = ["wake", "level1", "level2", "level3", "level4", "sleep"]
        
        # Add filler data for missing slots (e.g., using the previous slot's time/temp)
        last_dt = now.replace(hour=0, minute=0, second=0, microsecond=0) # Start of day
        last_temp = ECO_TEMPERATURE
        
        final_day_slots: List[tuple[str, datetime.datetime, float]] = []
        
        # Fill/Replace logic: Ensure all 6 slots are present, using ECO_TEMP if missing
        for name in required_levels:
            if name in temp_slot_map:
                dt, temp = temp_slot_map[name]
                final_day_slots.append((name, dt, temp))
            else:
                # Use a default time 1 minute after the last valid time for filler slots
                # This ensures chronological order even for unused slots.
                filler_dt = last_dt + datetime.timedelta(minutes=1)
                
                # Check if the filler time crossed midnight
                if filler_dt.date() > last_dt.date():
                    filler_dt = last_dt.replace(hour=23, minute=59, second=0) # Cap at 23:59
                    
                final_day_slots.append((name, filler_dt, ECO_TEMPERATURE))
            
            # Update last_dt for the next iteration
            last_dt = final_day_slots[-1][1] 


        # Re-sort the final list to ensure chronological order, using the name as a secondary key 
        # to preserve the wake/level/sleep order when times are identical.
        final_day_slots.sort(key=lambda x: x[1])

        # --- 4. Chronology Enforcer (The 1-Minute Rule) ---
        
        day_data: Dict[str, Any] = {}
        last_time_sent = datetime.time(hour=0, minute=0, second=0)
        
        for name, event_dt, temperature in final_day_slots:
            event_time = event_dt.time()
            
            # Check for chronological violation
            if event_time <= last_time_sent:
                # Calculate new time: 1 minute after the last time sent
                
                # Create a temporary datetime object using the last sent time
                temp_dt = event_dt.replace(hour=last_time_sent.hour, minute=last_time_sent.minute, second=0, microsecond=0)
                
                # Add 1 minute
                new_dt = temp_dt + datetime.timedelta(minutes=1)
                event_time = new_dt.time()
                
                logging.debug(f"[{day_name.upper()}] Time clash corrected for {name}. Original: {event_dt.strftime('%H:%M')}. New: {event_time.strftime('%H:%M')}")
                
            # Store the final, corrected level data
            day_data[name] = [
                event_time.strftime("%H:%M"),
                # FIX: Temperature is now a float (required by NeoHub)
                temperature,  
                5,  # Hysteresis
                True, # Active
            ]
            
            # Update the last time sent for the next iteration
            last_time_sent = event_time

        # Store the schedule for the day
        schedule_data[day_name] = day_data

    # The final schedule data is ready
    return schedule_data

# ... rest of the file ...

async def store_profile2(neohub_name: str, profile_name: str, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Stores a heating profile on the Neohub, ensuring single-escaped JSON string is passed."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")

    # 1. CREATE THE INNER COMMAND PAYLOAD (clean Python dict with float temps)
    inner_payload = {
        "STORE_PROFILE2": {
            "name": profile_name,
            "info": profile_data
        }
    }

    # 2. SERIALIZE TO A CLEAN JSON STRING
    # This ensures all quotes are DOUBLE QUOTES (") and all booleans are 'true'.
    command_json_string = json.dumps(inner_payload) 
    
    # 3. DEBUGGING ECHO: Log the final JSON string we are attempting to send.
    logging.debug(f"DEBUG: FINAL JSON Payload String for Hub: {command_json_string}")

    # 4. SEND THE COMMAND STRING DIRECTLY
    # This structure is necessary to bypass the neohubapi library's incorrect
    # internal serialization for complex nested commands.
    response = await send_command(neohub_name, command_json_string)
    return response

async def apply_schedule_to_heating(neohub_name: str, schedule_name: str, zones_config: Dict[str, Any]):
    """Applies the calculated schedule to all configured heating zones."""
    
    # 1. Fetch data
    outdoor_temp = await get_weather_data(OPENWEATHERMAP_CITY)
    churchsuite_data = await get_churchsuite_events()
    
    # Dummy live data for indoor temperature (for preheat calc)
    zones_live_data: Dict[str, Any] = {"outdoor_temp": outdoor_temp} 
    
    # For a real application, fetch live temp for all zones from the hub.
    # We will assume a default indoor temp for now if no live data is fetched.
    
    # 2. Iterate and apply schedule
    for zone_id, zone_config in zones_config.items():
        logging.info(f"Processing schedule for zone: {zone_config['zone_name']}")
        
        # Calculate the schedule data for the current zone
        profile_data = calculate_schedule(
            neohub_name, zone_config, churchsuite_data, zones_live_data
        )

        # Apply the profile to the hub
        response = await store_profile2(neohub_name, schedule_name, profile_data)
        
        if response and response.get("response") and "OK" in response["response"]:
            logging.info(f"Successfully stored profile '{schedule_name}' for zone {zone_config['zone_name']}.")
        else:
            logging.error(f"Failed to store profile '{schedule_name}' for zone {zone_config['zone_name']}. Response: {response}")

def validate_config(config: Dict[str, Any]) -> bool:
    """Validate the loaded configuration structure."""
    if not config.get("neohubs"):
        logging.error("Config missing 'neohubs' section.")
        return False
    # Add more validation logic as needed
    return True

async def update_heating_schedule():
    """Main function to run schedule updates."""
    config = load_config()
    if not config:
        logging.error("Configuration failed to load. Cannot update schedule.")
        return
        
    for neohub_name, neohub_config in config["neohubs"].items():
        schedule_name = neohub_config.get("schedule_name", "Current Week")
        zones_config = neohub_config.get("zones", {})
        
        if not zones_config:
            logging.warning(f"No zones configured for Neohub: {neohub_name}. Skipping.")
            continue
            
        await apply_schedule_to_heating(neohub_name, schedule_name, zones_config)
        
def main():
    """Application entry point."""
    config = load_config()
    if not config:
        logging.error("Configuration failed to load. Exiting.")
        return
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")

    for neohub_name, neohub_config in config["neohubs"].items():
        if not connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()

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
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main()
    