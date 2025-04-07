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
from neohubapi.neohub import NeoHub, NeoHubUsageError, NeoHubConnectionError # type: ignore

# Configuration
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")
PREHEAT_TIME_MINUTES = int(os.environ.get("PREHEAT_TIME_MINUTES", 30))
DEFAULT_TEMPERATURE = int(os.environ.get("DEFAULT_TEMPERATURE", 19))
ECO_TEMPERATURE = int(os.environ.get("ECO_TEMPERATURE", 12))
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

def validate_config(config: Dict[str, Any]) -> bool:
    if "neohubs" not in config or not config["neohubs"]:
        logging.error("No Neohubs found in configuration.")
        return False
    if "locations" not in config or not config["locations"]:
        logging.error("No locations found in configuration.")
        return False
    return True

async def send_command(neohub_name: str, command: Dict[str, Any]) -> Optional[Any]:
    """Sends a command to the Neohub using neohubapi."""
    global hubs
    hub = hubs.get(neohub_name)
    if hub is None:
        logging.error(f"Not connected to Neohub: {neohub_name}")
        return None
    try:
        response = await hub._send(command)
        return response
    except (NeoHubUsageError, NeoHubConnectionError) as e:
        logging.error(f"Error sending command to Neohub {neohub_name}: {e}")
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
    response = await send_command(neohub_name, command)
    return response


async def get_profile(neohub_name: str, profile_name: str) -> Optional[Dict[str, Any]]:
    """Retrieves a heating profile from the Neohub using neohubapi."""
    logging.info(f"Getting profile {profile_name} from Neohub {neohub_name}")
    command = {"GET_PROFILE": profile_name}
    response = await send_command(neohub_name, command)
    return response



def close_connections() -> None:
    """Closes all Neohub connections."""
    global hubs
    for neohub_name, hub in hubs.items():
        if hub:
            asyncio.run(hub._client.disconnect()) #changed
            logging.info(f"Disconnected from Neohub: {neohub_name}")
    hubs = {}



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

    start_time = datetime.datetime.fromisoformat(start_time_str)
    end_time = datetime.datetime.fromisoformat(end_time_str)
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

    def add_event(
        day_data: Dict[str, Any],
        event_name: str,
        event_time: datetime.datetime,
        temperature: float,
    ):
        """Adds an event to the day's schedule, handling the 6-event limit."""
        if len(day_data) < 6:
            day_data[event_name] = [
                event_time.strftime("%H:%M"),
                temperature,
                0,
                False,
            ]
            if LOGGING_LEVEL == "DEBUG":
                logging.debug(
                    f"calculate_schedule.add_event: Added event {event_name} at {event_time.strftime('%H:%M')} with temp {temperature}"
                )
            return True
        return False

    for day in days:
        day_schedule = profile_data[day]
        add_event(day_schedule, "wake", start_time - preheat_time, DEFAULT_TEMPERATURE)
        add_event(day_schedule, "end", end_time, ECO_TEMPERATURE)
        if len(day_schedule) < 6:
            last_event_time = end_time
            last_event_temp = ECO_TEMPERATURE
            for i in range(len(day_schedule), 6):
                fill_event_name = f"fill_{i}"
                add_event(day_schedule, fill_event_name, last_event_time, last_event_temp)
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"calculate_schedule: Calculated schedule: {profile_data}")
    return profile_data



async def apply_schedule_to_heating(
    neohub_name: str, profile_name: str, schedule_data: Dict[str, Any]
) -> None:
    """Applies the heating schedule to the Heatmiser system by storing the profile."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(
            f"apply_schedule_to_heating: neohub_name={neohub_name}, profile_name={profile_name}, schedule_data={schedule_data}"
        )
    response = await store_profile(neohub_name, profile_name, schedule_data)
    if response:
        logging.info(
            f"Successfully stored profile {profile_name} on Neohub {neohub_name}"
        )
    else:
        logging.error(f"Failed to store profile {profile_name} on Neohub {neohub_name}")



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



async def update_heating_schedule() -> None:
    """Updates the heating schedule based on upcoming bookings."""
    global config
    if config is None:
        logging.error("Configuration not loaded.  Exiting.")
        return
    # Validate the configuration
    if not validate_config(config):
        logging.error("Invalid configuration. Exiting.")
        return
    # Debug log to confirm config structure
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(f"update_heating_schedule: config['locations'] = {config.get('locations')}")
        logging.debug(f"update_heating_schedule: config['neohubs'] = {config.get('neohubs')}")
    # Use an environment variable specifically for the timezone.
    #  Example: "Europe/London" or "America/New_York"
    location_timezone_name = os.environ.get("CHURCHSUITE_TIMEZONE", "Europe/London")
    try:
        location_timezone = pytz.timezone(location_timezone_name)
    except pytz.exceptions.UnknownTimeZoneError:
        logging.error(
            f"Timezone '{location_timezone_name}' is invalid.  Defaulting to Europe/London.  "
            "Please set the CHURCHSUITE_TIMEZONE environment variable with a valid timezone name (e.g., 'Europe/London')."
        )
        location_timezone = pytz.timezone("Europe/London")

    today = datetime.datetime.now(location_timezone).replace(tzinfo=None) #changed
    current_week_start = today - datetime.timedelta(days=today.weekday())
    current_week_end = current_week_start + datetime.timedelta(days=6)
    next_week_start = current_week_end + datetime.timedelta(days=1)
    next_week_end = next_week_start + datetime.timedelta(days=6)
    if LOGGING_LEVEL == "DEBUG":
        logging.debug(
            f"update_heating_schedule: today={today}, current_week_start={current_week_start}, current_week_end={current_week_end}, next_week_start={next_week_start}, next_week_end={next_week_end}"
        )

    data = get_bookings_and_locations()
    if data:
        booked_resources = data.get("booked_resources", [])
        resources = data.get("resources", [])  # Get the resources list

        # Debug log to confirm fetched data
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"update_heating_schedule: booked_resources = {booked_resources}")
            logging.debug(f"update_heating_schedule: resources = {resources}")
        if not booked_resources:
            logging.info("No bookings to process.")
            return

        if not resources:
            logging.info("No resources to process.")
            return

        # Create a mapping of resource_id to resource name.
        resource_map = {r["id"]: r["name"] for r in resources}
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(f"update_heating_schedule: resource_map = {resource_map}")

        current_week_bookings = []
        next_week_bookings = []
        # After categorizing bookings
        if LOGGING_LEVEL == "DEBUG":
            logging.debug(
                f"update_heating_schedule: current_week_bookings = {current_week_bookings}"
            )
            logging.debug(
                f"update_heating_schedule: next_week_bookings = {next_week_bookings}"
            )
        for booking in booked_resources:
            start_time_str = booking.get("starts_at")
            if start_time_str:
                # Parse the start time, using dateutil.parser which handles more formats
                parsed_dt = dateutil.parser.parse(start_time_str)

                # If the parsed datetime is naive (no timezone info), assume UTC
                if parsed_dt.tzinfo is None or parsed_dt.utcoffset() is None:
                    utc_dt = parsed_dt.replace(tzinfo=pytz.utc)
                    logging.warning(f"Booking time for {booking} was naive, assuming UTC")
                else:
                    utc_dt = parsed_dt.astimezone(pytz.utc)

                # Convert the booking time to the location timezone.
                local_start_dt = utc_dt.astimezone(location_timezone).replace(tzinfo=None)

                if current_week_start <= local_start_dt <= current_week_end:
                    current_week_bookings.append(booking)
                elif next_week_start <= local_start_dt <= next_week_end:
                    next_week_bookings.append(booking)
            else:
                logging.warning(f"Booking with id {booking.get('id', 'unknown')} has no 'starts_at' time.")

        if LOGGING_LEVEL == "DEBUG":
            logging.debug(
                f"update_heating_schedule: current_week_bookings={current_week_bookings}, next_week_bookings={next_week_bookings}"
            )

        # Iterate through locations defined in config.json
        for location_name, location_config in config["locations"].items():
            neohub_name = location_config["neohub"]
            resource_ids = [resource_id for resource_id, name in resource_map.items() if name == location_name]

            # Process current week bookings
            for booked_resource in current_week_bookings:
                if booked_resource["resource_id"] in resource_ids:
                    if LOGGING_LEVEL == "DEBUG":
                        logging.debug(f"update_heating_schedule: Processing current week booking for location_name = {location_name}")
                    if not await check_neohub_compatibility(config, neohub_name):
                        logging.error(
                            f"Neohub {neohub_name} is not compatible with the required schedule format.  Skipping."
                        )
                        continue
                    external_temperature = get_external_temperature()
                    schedule_data = calculate_schedule(booked_resource, config, external_temperature, resource_map)
                    if LOGGING_LEVEL == "DEBUG":
                        logging.debug(f"update_heating_schedule: schedule_data = {schedule_data}")
                    if schedule_data:
                        await apply_schedule_to_heating(
                            neohub_name, "Current Week", schedule_data
                        )

            # Process next week bookings
            for booked_resource in next_week_bookings:
                if booked_resource["resource_id"] in resource_ids:
                    if LOGGING_LEVEL == "DEBUG":
                        logging.debug(f"update_heating_schedule: Processing next week booking for location_name = {location_name}")
                    if not await check_neohub_compatibility(config, neohub_name):
                        logging.error(
                            f"Neohub {neohub_name} is not compatible with the required schedule format.  Skipping."
                        )
                        continue
                    external_temperature = get_external_temperature()
                    schedule_data = calculate_schedule(booked_resource, config, external_temperature, resource_map)
                    if schedule_data:
                        await apply_schedule_to_heating(
                            neohub_name, "Next Week", schedule_data
                        )

        for neohub_name in set(config["neohubs"].keys()):
            command = {"RUN_PROFILE": "Current Week"}
            response = await send_command(neohub_name, command)
            if response:
                logging.info(
                    f"Successfully set profile 'Current Week' as active on Neohub {neohub_name}."
                )
                if LOGGING_LEVEL == "DEBUG":
                    logging.debug(
                        f"update_heating_schedule:  Sent RUN_PROFILE for Current Week to {neohub_name}"
                    )
            else:
                logging.error(
                    f"Failed to set profile 'Current Week' as active on Neohub {neohub_name}."
                )
    else:
        logging.info("No data received from ChurchSuite.")





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
        close_connections()
        logging.info("Exiting...")



if __name__ == "__main__":
    main()

