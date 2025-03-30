import datetime
import json
import asyncio
import logging
import time
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import websockets
import argparse  # Import the argparse module
import os  # Import the os module
import ssl  # Import the ssl module
from typing import Dict, Any, List, Optional

# Configuration
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
OPENWEATHERMAP_CITY = os.environ.get("OPENWEATHERMAP_CITY")
CHURCHSUITE_URL = os.environ.get("CHURCHSUITE_URL")  # Combined URL
PREHEAT_TIME_MINUTES = int(os.environ.get("PREHEAT_TIME_MINUTES", 30))
DEFAULT_TEMPERATURE = int(os.environ.get("DEFAULT_TEMPERATURE", 19))  # Changed to 19
ECO_TEMPERATURE = int(os.environ.get("ECO_TEMPERATURE", 12))
TEMPERATURE_SENSITIVITY = int(os.environ.get("TEMPERATURE_SENSITIVITY", 10))
PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE = float(
    os.environ.get("PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE", 5)
)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config/config.json")

# Neohub Configuration from Environment Variables
NEOHUBS = {}
neohub_count = 1
while True:
    neohub_name = os.environ.get(f"NEOHUB_{neohub_count}_NAME")
    neohub_address = os.environ.get(f"NEOHUB_{neohub_count}_ADDRESS")
    neohub_port = os.environ.get(f"NEOHUB_{neohub_count}_PORT", "4243")  # Default port
    neohub_token = os.environ.get(f"NEOHUB_{neohub_count}_TOKEN")
    if not neohub_name or not neohub_address or not neohub_token:
        if neohub_count == 1:
            logging.warning(
                "No Neohub configuration found in environment variables.  Ensure NEOHUB_1_NAME, NEOHUB_1_ADDRESS, and NEOHUB_1_TOKEN are set."
            )
        break  # Stop if any essential variable is missing
    NEOHUBS[neohub_name] = {
        "address": neohub_address,
        "port": int(neohub_port),
        "token": neohub_token,
    }
    neohub_count += 1
if not NEOHUBS:
    logging.warning("No Neohub configurations were loaded.")
    # Global variables
neohub_connections = {}  # Store websocket connections for each Neohub
config = None  # Make config a global variable


def load_config(config_file: str) -> Optional[Dict[str, Any]]:
    """Loads configuration data from a JSON file."""
    try:
        with open(config_file, "r") as f:
            loaded_config = json.load(f)
            # Merge Neohub config from environment variables into the loaded config.
            #  This allows for a hybrid approach, where some neohubs might be in the file, and others in env vars.
            if "neohubs" in loaded_config:
                loaded_config["neohubs"].update(NEOHUBS)
            else:
                loaded_config["neohubs"] = (
                    NEOHUBS  # If there are no neohubs, set to the env vars
                )
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


async def connect_to_neohub(
    neohub_name: str, neohub_config: Dict[str, Any]
) -> bool:
    """Connects to a Neohub via websocket and authenticates."""
    global neohub_connections
    uri = f"wss://{neohub_config['address']}:{neohub_config['port']}"
    try:
        # Create an SSL context that doesn't require certificate verification.
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # Or use a specific protocol
        ssl_context.check_hostname = False  # Disable hostname checking
        ssl_context.verify_mode = ssl.CERT_NONE  # Don't verify the server's certificate

        # Pass the SSL context to websockets.connect()
        ws = await websockets.connect(uri, ssl=ssl_context)
        logging.info(f"Connected to Neohub: {neohub_name}")
        neohub_connections[neohub_name] = ws  # Store the connection
        return True
    except Exception as e:
        logging.error(f"Error connecting to Neohub {neohub_name}: {e}")
        return False


async def send_command(
    neohub_name: str, command: Dict[str, Any], command_id: int = 1
) -> Optional[Dict[str, Any]]:
    """Sends a command to the specified Neohub."""
    global neohub_connections, config
    if (
        neohub_name not in neohub_connections
        or neohub_connections[neohub_name].closed
    ):
        logging.error(
            f"Not connected to Neohub: {neohub_name}.  Attempting to reconnect..."
        )
        # Try to reconnect
        if config and neohub_name in config["neohubs"]:
            if not await connect_to_neohub(
                neohub_name, config["neohubs"][neohub_name]
            ):
                logging.error(f"Failed to reconnect to Neohub {neohub_name}.")
                return None
        else:
            logging.error(
                f"Neohub {neohub_name} not found in config, or config error."
            )
            return None

    ws = neohub_connections[neohub_name]
    message = {
        "message_type": "hm_get_command_queue",
        "message": json.dumps(
            {
                "token": config["neohubs"][neohub_name]["token"],
                "COMMANDS": [{"COMMAND": command, "COMMANDID": command_id}],
            }
        ),
    }
    try:
        await ws.send(json.dumps(message))
        response = await ws.recv()
        return json.loads(response)
    except Exception as e:
        logging.error(f"Error sending command to Neohub {neohub_name}: {e}")
        return None



async def get_zones(neohub_name: str) -> Optional[List[str]]:
    """Retrieves zone names from the Neohub."""
    command = {"GET_ZONES": 0}
    response = await send_command(neohub_name, command)
    if response:
        try:
            zones = response["response"]
            return zones
        except KeyError:
            logging.error(
                f"Unexpected response format for GET_ZONES from {neohub_name}"
            )
            return None
    return None


async def set_temperature(neohub_name: str, zone_name: str, temperature: float) -> Optional[Dict[str, Any]]:
    """Sets the temperature for a specified zone."""
    command = {"SET_TEMP": [temperature, zone_name]}
    response = await send_command(neohub_name, command)
    if response:
        return response
    return None


async def get_live_data(neohub_name: str) -> Optional[Dict[str, Any]]:
    """Gets the live data."""
    command = {"GET_LIVE_DATA": 0}
    response = await send_command(neohub_name, command)
    if response:
        return response
    return None


async def store_profile(
    neohub_name: str, profile_name: str, profile_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Stores a heating profile on the Neohub."""
    command = {"STORE_PROFILE": {"name": profile_name, "info": profile_data}}
    response = await send_command(neohub_name, command)
    if response:
        return response
    return None


async def get_profile(neohub_name: str, profile_name: str) -> Optional[Dict[str, Any]]:
    """Retrieves a heating profile from the Neohub."""
    command = {"GET_PROFILE": profile_name}
    response = await send_command(neohub_name, command)
    if response:
        try:
            # Assuming the profile data is nested within the response
            profile_data = response["response"]  # Adjust key as necessary
            return response["response"]
        except KeyError:
            logging.error(
                f"Unexpected response format for GET_PROFILE from {neohub_name}"
            )
            return None
    return None



async def close_connections() -> None:
    """Closes all websocket connections."""
    global neohub_connections
    for neohub_name, ws in neohub_connections.items():
        if ws:
            await ws.close()
            logging.info(f"Disconnected from Neohub: {neohub_name}")
    neohub_connections = {}


def get_external_temperature() -> Optional[float]:
    """Gets the current external temperature."""
    try:
        response = requests.get(
            f"https://api.openweathermap.org/data/2.5/weather?q={OPENWEATHERMAP_CITY}&appid={OPENWEATHERMAP_API_KEY}&units=metric"
        )
        response.raise_for_status()
        data = response.json()
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
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching JSON data from {url}: {e}")
        return None



def get_bookings_and_locations() -> Optional[Dict[str, Any]]:
    """Fetches bookings and locations data from ChurchSuite."""
    return get_json_data(CHURCHSUITE_URL)



def calculate_schedule(
    booking: Dict[str, Any], config: Dict[str, Any], external_temperature: Optional[float]
) -> Optional[Dict[str, Any]]:
    """Calculates the heating schedule for a single booking."""
    location_name = booking["location"]
    if location_name not in config["locations"]:
        logging.error(f"Location '{location_name}' not found in configuration.")
        return None

    location_config = config["locations"][location_name]
    neohub_name = location_config["neohub"]  # Get Neohub name
    zones = location_config["zones"]
    heat_loss_factor = location_config["heat_loss_factor"]
    min_external_temp = location_config["min_external_temp"]

    start_time = datetime.datetime.fromisoformat(booking["start_time"])
    end_time = datetime.datetime.fromisoformat(booking["end_time"])
    preheat_time = datetime.timedelta(minutes=PREHEAT_TIME_MINUTES)

    # Adjust preheat time based on external temperature
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

    # Construct profile data structure
    profile_data = {}
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for day in days:
        profile_data[day] = {}  # Initialize each day

    # Create a function to add events, handling the 6-event limit
    def add_event(day_data: Dict[str, Any], event_name: str, event_time: datetime.datetime, temperature: float):
        """Adds an event to the day's schedule, handling the 6-event limit."""
        if len(day_data) < 6:
            day_data[event_name] = [
                event_time.strftime("%H:%M"),
                temperature,
                0,  # Assume 0 for some flag
                False, # Assume False for some flag
            ]
            return True
        return False # Indicate that the event was not added

    # Add events for each day.  For simplicity, assume the same schedule for all days.
    for day in days:
        day_schedule = profile_data[day]
        add_event(day_schedule, "wake", start_time - preheat_time, DEFAULT_TEMPERATURE)
        add_event(day_schedule, "end", end_time, ECO_TEMPERATURE)

        # If there are less than 6 events, fill the remaining slots.
        if len(day_schedule) < 6:
            last_event_time = end_time
            last_event_temp = ECO_TEMPERATURE
            for i in range(len(day_schedule), 6):
                # Create a generic name like "fill_x"
                fill_event_name = f"fill_{i}"
                add_event(day_schedule, fill_event_name, last_event_time, last_event_temp)
    return profile_data  # Return the profile data



async def apply_schedule_to_heating(neohub_name: str, profile_name: str, schedule_data: Dict[str, Any]) -> None:
    """Applies the heating schedule to the Heatmiser system by storing the profile."""
    logging.info(f"Storing profile {profile_name} on Neohub {neohub_name}")
    response = await store_profile(neohub_name, profile_name, schedule_data)

    if response:
        logging.info(f"Successfully stored profile {profile_name} on Neohub {neohub_name}")
    else:
        logging.error(f"Failed to store profile {profile_name} on Neohub {neohub_name}")



async def check_neohub_compatibility(neohub_name: str) -> bool:
    """
    Checks if the Neohub is compatible with the required schedule format (7-day, 6 events).
    Returns True if compatible, False otherwise.
    """
    # Use GET_PROFILE_0 to retrieve profile data
    profile_data = await get_profile(neohub_name, "0")  # Get profile 0

    if profile_data:
        # Check if it is a 7 day profile
        if len(profile_data.keys()) != 7:
            logging.error(f"Neohub {neohub_name} is not configured for a 7-day schedule.")
            return False

        # Check number of events per day.
        for day in profile_data:
            if len(profile_data[day]) != 6:
                logging.error(
                    f"Neohub {neohub_name} does not have 6 events per day. Found {len(profile_data[day])} for {day}."
                )
                return False
    else:
        logging.error(
            f"Failed to retrieve profile data from Neohub {neohub_name} to check compatibility."
        )
        return False
    return True



def update_heating_schedule() -> None:
    """Updates the heating schedule based on upcoming bookings."""
    global config
    if config is None:
        logging.error("Configuration not loaded.  Exiting.")
        return

    today = datetime.datetime.now()
    current_week_start = today - datetime.timedelta(days=today.weekday())  # Start of current week (Monday)
    current_week_end = current_week_start + datetime.timedelta(days=6)  # End of current week (Sunday)
    next_week_start = current_week_end + datetime.timedelta(days=1)  # Start of next week (Monday)
    next_week_end = next_week_start + datetime.timedelta(days=6)

    data = get_bookings_and_locations()
    if data:
        bookings = data.get("bookings", [])
        locations = data.get("locations", [])

        if not bookings:
            logging.info("No bookings to process.")
            return

        if not locations:
            logging.info("No locations to process.")
            return

        # Separate bookings for current and next week
        current_week_bookings = [
            b
            for b in bookings
            if current_week_start <= datetime.datetime.fromisoformat(b["start_time"]) <= current_week_end
        ]
        next_week_bookings = [
            b
            for b in bookings
            if next_week_start <= datetime.datetime.fromisoformat(b["start_time"]) <= next_week_end
        ]

        neohub_names = set() # set to hold unique neohub names
        # Process current week bookings
        for booking in current_week_bookings:
            location_name = booking["location"]
            neohub_name = config["locations"][location_name]["neohub"]
            neohub_names.add(neohub_name) # add neohub name

            # Check compatibility before proceeding
            if not asyncio.run(check_neohub_compatibility(neohub_name)):
                logging.error(
                    f"Neohub {neohub_name} is not compatible with the required schedule format.  Please adjust its settings."
                )
                continue  # Skip this Neohub and booking

            external_temperature = asyncio.run(get_external_temperature())
            schedule_data = calculate_schedule(booking, config, external_temperature)
            if schedule_data:
                asyncio.run(apply_schedule_to_heating(neohub_name, "Current Week", schedule_data))

        # Process next week bookings
        for booking in next_week_bookings:
            location_name = booking["location"]
            neohub_name = config["locations"][location_name]["neohub"]
            neohub_names.add(neohub_name)

            # Check compatibility before proceeding.  This check is now redundant, but kept for consistency.
            if not asyncio.run(check_neohub_compatibility(neohub_name)):
                logging.error(
                    f"Neohub {neohub_name} is not compatible with the required schedule format.  Please adjust its settings."
                )
                continue  # Skip this Neohub and booking

            external_temperature = asyncio.run(get_external_temperature())
            schedule_data = calculate_schedule(booking, config, external_temperature)
            if schedule_data:
                asyncio.run(apply_schedule_to_heating(neohub_name, "Next Week", schedule_data))

        # After processing all bookings, set "Current Week" as active.
        for neohub_name in neohub_names:
            command = {"RUN_PROFILE": "Current Week"}  # Set "Current Week" as active
            response = asyncio.run(send_command(neohub_name, command))
            if response:
                logging.info(f"Successfully set profile 'Current Week' as active on Neohub {neohub_name}.")
            else:
                logging.error(f"Failed to set profile 'Current Week' as active on Neohub {neohub_name}.")
    else:
        logging.info("No data received from ChurchSuite.")





async def main() -> None:
    """Main application function."""
    logging.basicConfig(level=logging.INFO)

    # Parse command-line arguments
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

    # Load configuration
    global config
    config = load_config(config_file)
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return

    # Connect to all Neohubs on startup
    for neohub_name, neohub_config in config["neohubs"].items():
        if not await connect_to_neohub(neohub_name, neohub_config):
            logging.error(f"Failed to connect to Neohub: {neohub_name}. Exiting.")
            exit()

    # Get zones from all connected Neohubs
    for neohub_name in config["neohubs"]:
        zones = await get_zones(neohub_name)
        if zones:
            logging.info(f"Zones on {neohub_name}: {zones}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(update_heating_schedule, "interval", minutes=60)
    scheduler.start()

    try:
        while True:
            time.sleep(600)
    except KeyboardInterrupt:
        logging.info("Shutting down scheduler...")
        scheduler.shutdown()
        logging.info("Closing Neohub connections...")
        await close_connections()
        logging.info("Exiting...")


if __name__ == "__main__":
    asyncio.run(main())
