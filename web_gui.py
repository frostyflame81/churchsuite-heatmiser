import os
import json
import logging
import logging.handlers
import datetime
import glob
from flask import Flask, render_template, request, jsonify
import asyncio
from typing import Dict, Any

# Configure Flask logging
gui_formatter = logging.Formatter('%(asctime)s | gui | %(levelname)s | %(message)s')
gui_handler = logging.handlers.TimedRotatingFileHandler("/app/logs/scheduler.log", when="D", interval=1, backupCount=7)
gui_handler.setFormatter(gui_formatter)

logging.basicConfig(level=logging.INFO, handlers=[gui_handler, logging.StreamHandler()])
logger = logging.getLogger("gui")

# Constants
MANUAL_RUN_FLAG = '/tmp/manual_run_flag'
CONFIG_RELOAD_FLAG = '/tmp/config_reload_flag'
SCHEDULER_STATUS_FILE = '/tmp/scheduler_status.json'
CONFIG_FILE = '/config/config.json'

# --------------------------------------------------------------------------
# 1. FLASK SETUP
# --------------------------------------------------------------------------
app = Flask(__name__)

# --------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# --------------------------------------------------------------------------

def full_status_prep(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formats the raw scheduler status into the complete dictionary used by both the template and the API.
    Returns: The 'full_status' dictionary.
    """
    overall_status = raw_data.get("overall_status", "UNAVAILABLE")
    
    # The returned dictionary is the 'full_status'
    full_status = {
        "overall_status": overall_status,
        "display": map_status_to_display(overall_status),
        "timestamp": raw_data.get("last_run_time", "N/A"),
        "neohub_reports": raw_data.get("neohub_reports", [])
    }
    return full_status

def get_structured_config() -> Dict[str, Any]:
    """Reads the structured config.json file, ensuring global_settings are present."""
    default_global_settings = {
        "PREHEAT_TIME_MINUTES": 30,
        "DEFAULT_TEMPERATURE": 18,
        "ECO_TEMPERATURE": 12,
        "TEMPERATURE_SENSITIVITY": 10,
        "PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE": 5
    }

    try:
        # Load the existing config file
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Create a base structure if the file is missing or corrupt
        logging.warning(f"Config file {CONFIG_FILE} not found or corrupted. Creating base structure.")
        config = {"locations": {}}

    # 1. Ensure 'global_settings' key exists and merge defaults
    if 'global_settings' not in config or not isinstance(config['global_settings'], dict):
        config['global_settings'] = {}
    
    # Merge existing global settings with any new defaults defined in the code
    config['global_settings'] = {**default_global_settings, **config['global_settings']}

    # 2. Ensure 'hub_settings' key exists (NEW)
    if 'hub_settings' not in config:
         config['hub_settings'] = {}

    # 3. Ensure 'locations' key exists
    if 'locations' not in config:
         config['locations'] = {}

    # 4. Ensure 'zone_properties' key exists (NEW)
    if 'zone_properties' not in config:
        config['zone_properties'] = {}

    return config

def _normalize_numbers_to_float(config_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensures that specific configuration values are stored as floats (e.g., 3 becomes 3.0),
    which guarantees the correct type is written to the JSON file.
    """
    
    # Keys in global_settings that must be floats
    GLOBAL_FLOAT_KEYS = [
        "PREHEAT_TIME_MINUTES", 
        "DEFAULT_TEMPERATURE", 
        "ECO_TEMPERATURE", 
        "TEMPERATURE_SENSITIVITY", 
        "PREHEAT_ADJUSTMENT_MINUTES_PER_DEGREE"
    ]

    # Keys in hub_settings that must be floats
    HUB_FLOAT_KEYS = [
        "HEAT_LOSS_CONSTANT"
    ]

    # Keys in locations that must be floats
    LOCATION_FLOAT_KEYS = [
        "heat_loss_factor", 
        "min_external_temp"
    ]

    # Keys in zone_properties that must be floats
    ZONE_FLOAT_KEYS = [
        "heat_loss_factor"
    ]
    
    # --- 1. Process global_settings ---
    global_settings = config_data.get("global_settings", {})
    for key in GLOBAL_FLOAT_KEYS:
        if key in global_settings:
            try:
                # Explicitly cast the value to float
                global_settings[key] = float(global_settings[key])
            except (ValueError, TypeError):
                # Log error and ignore if conversion fails (e.g., if user enters text)
                logging.warning(f"Failed to cast global setting '{key}' value '{global_settings[key]}' to float.")
                pass 

    # --- 2. Process hub_settings ---
    hub_settings = config_data.get("hub_settings", {})
    for hub_data in hub_settings.values():
        for key in HUB_FLOAT_KEYS:
            if key in hub_data:
                try:
                    # Explicitly cast the value to float
                    hub_data[key] = float(hub_data[key])
                except (ValueError, TypeError):
                    logging.warning(f"Failed to cast hub setting '{key}' value '{hub_data[key]}' to float.")
                    pass

    # --- 3. Process locations ---
    locations = config_data.get("locations", {})
    for location_data in locations.values():
        for key in LOCATION_FLOAT_KEYS:
            if key in location_data:
                try:
                    # Explicitly cast the value to float
                    location_data[key] = float(location_data[key])
                except (ValueError, TypeError):
                    logging.warning(f"Failed to cast location setting '{key}' value '{location_data[key]}' to float.")
                    pass 

    # --- 4. Process zone_properties ---
    zone_properties = config_data.get("zone_properties", {})
    # Iterate over the zone names (keys) and their properties (values)
    for zone_data in zone_properties.values():
        for key in ZONE_FLOAT_KEYS:
            if key in zone_data:
                try:
                    # Explicitly cast the value to float
                    zone_data[key] = float(zone_data[key])
                except (ValueError, TypeError):
                    logging.warning(f"Failed to cast zone property '{key}' value '{zone_data[key]}' to float.")
                    pass

    return config_data

def write_structured_config(update_data: Dict[str, Any]) -> bool:
    """
    Writes the configuration data back to config.json by performing a
    non-destructive merge with the current configuration (read via 
    get_structured_config).
    """
    try:
        # 1. Read the current configuration from the file, ensuring defaults are applied
        current_full_config = get_structured_config()

        # 2. Perform the Non-Destructive Merge
        # Start with the full, default-filled config
        merged_config = current_full_config.copy()
        
        # Overwrite/add keys from the incoming update_data (e.g., 'zone_properties')
        for key, value in update_data.items():
            merged_config[key] = value

        # 3. Write the merged configuration back to the file
        with open(CONFIG_FILE, 'w') as f:
            # Use indent=4 for a human-readable config file
            json.dump(merged_config, f, indent=4) 
        
        return True
    except Exception as e:
        logging.error(f"Failed to write structured config file after merge: {e}")
        return False
    
def get_scheduler_status() -> Dict[str, Any]:
    """Reads the detailed scheduler status from the temporary JSON file.

    Includes comprehensive error handling and logging for robustness.
    """
    SCHEDULER_STATUS_FILE = '/tmp/scheduler_status.json'
    # Define a default status to return in case of any failure
    default_status = {"overall_status": "UNAVAILABLE", "timestamp": "N/A", "details": {}, "summary": "Scheduler status file is unavailable."}
    
    try:
        with open(SCHEDULER_STATUS_FILE, 'r') as f:
            status = json.load(f)
            return status
    except FileNotFoundError:
        # File not existing is often expected (e.g., scheduler hasn't run yet).
        # We can log this as an info/warning rather than an error.
        logging.info(f"Scheduler status file not yet found at {SCHEDULER_STATUS_FILE}.")
        return default_status
    except json.JSONDecodeError as e:
        # The file exists but is corrupted (not valid JSON). This is a fault.
        logging.error(f"Scheduler status file corrupted ({SCHEDULER_STATUS_FILE}). JSON Decode Error: {e}")
        return default_status
    except Exception as e:
        # Catch all other I/O errors (e.g., PermissionError, IOError)
        logging.error(f"An unexpected error occurred while reading scheduler status file {SCHEDULER_STATUS_FILE}: {e}")
        return default_status

def map_status_to_display(overall_status: str) -> Dict[str, str]:
    """Helper function to map the status string to display text and color."""
    
    # Default to Total Failure for safety if status is unexpected
    status_display = {
        "text": "TOTAL FAILURE",
        "color": "bg-danger" 
    }
    
    if overall_status == "SUCCESS":
        status_display = {"text": "SUCCESS", "color": "bg-success"}
    elif overall_status == "PARTIAL_FAILURE":
        status_display = {"text": "PARTIAL FAILURE", "color": "bg-warning"}
    elif overall_status == "FAILURE":
        status_display = {"text": "TOTAL FAILURE", "color": "bg-danger"}
    
    # FIX: Explicitly handle the UNAVAILABLE state
    elif overall_status == "UNAVAILABLE":
        # Neutral status when the scheduler hasn't run or file couldn't be read.
        status_display = {"text": "UNAVAILABLE", "color": "bg-white text-dark"}
        
    return status_display

def trigger_manual_run_in_scheduler() -> bool:
    """Creates a flag file that app.py monitors to trigger a manual run."""
    try:
        # Create an empty file to act as the signal
        open(MANUAL_RUN_FLAG, 'w').close()
        logging.info(f"Created manual run flag at {MANUAL_RUN_FLAG}")
        return True
    except Exception as e:
        logging.error(f"Failed to create manual run flag: {e}")
        return False

def reload_config_in_scheduler() -> bool:
    """Creates a flag file that app.py monitors to reload configuration."""
    try:
        # Create an empty file to act as the signal
        open(CONFIG_RELOAD_FLAG, 'w').close()
        logging.info(f"Created config reload flag at {CONFIG_RELOAD_FLAG}")
        return True
    except Exception as e:
        logging.error(f"Failed to create config reload flag: {e}")
        return False

# --------------------------------------------------------------------------
# 3. FLASK ROUTES (API & Views)
# --------------------------------------------------------------------------

@app.route('/')
def dashboard():
    """
    Renders the main dashboard page, gathering all necessary data (config, PID, and scheduler status).
    Uses the unified full_status object for clean status reporting.
    """
    config_data = get_structured_config()
    scheduler_pid = os.getppid() 
    full_status = full_status_prep(get_scheduler_status())
    
    return render_template(
        'dashboard.html', 
        config=config_data, 
        scheduler_pid=scheduler_pid,
        status=full_status # Pass the unified status object
    )

@app.route('/api/logs', methods=['GET'])
def get_logs():
    level_filter = request.args.get('level', 'ALL')        # INFO, DEBUG, ALL
    source_filter = request.args.getlist('sources')       # ['scheduler', 'gui']
    timeframe = request.args.get('timeframe', '24h')      # 60m, 24h, 7d
    page = request.args.get('page', default=1, type=int)
    per_page = 50

    # 1. Determine which files to read (7d requires checking backups)
    log_files = ["/app/logs/scheduler.log"]
    if timeframe == '7d':
        log_files += glob.glob("/app/logs/scheduler.log.*")
    
    all_parsed_logs = []
    cutoff_time = datetime.datetime.now()
    if timeframe == '60m': cutoff_time -= datetime.timedelta(minutes=60)
    elif timeframe == '24h': cutoff_time -= datetime.timedelta(hours=24)
    else: cutoff_time -= datetime.timedelta(days=7)

    for file_path in log_files:
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    parts = line.split(' | ')
                    if len(parts) < 4: continue
                    
                    log_ts = datetime.datetime.strptime(parts[0].split(',')[0], '%Y-%m-%d %H:%M:%S')
                    log_source = parts[1].strip()
                    log_level = parts[2].strip()
                    log_msg = parts[3].strip()

                    # Apply Filters
                    if log_ts < cutoff_time: continue
                    if level_filter != 'ALL' and log_level != level_filter: continue
                    if source_filter and log_source not in source_filter: continue

                    all_parsed_logs.append({
                        "ts": parts[0], "src": log_source, "lvl": log_level, "msg": log_msg
                    })
        except Exception: continue

    # Sort by newest first
    all_parsed_logs.sort(key=lambda x: x['ts'], reverse=True)

    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    paginated_logs = all_parsed_logs[start:end]

    return jsonify({
        "success": True,
        "logs": paginated_logs,
        "total_pages": (len(all_parsed_logs) // per_page) + 1,
        "current_page": page
    })

@app.route('/api/trigger-manual', methods=['POST'])
def api_trigger_manual():
    """API endpoint to trigger a manual schedule update."""
    if trigger_manual_run_in_scheduler():
        return jsonify({"success": True, "message": "Manual run signal sent to scheduler."}), 200
    else:
        return jsonify({"success": False, "message": "Failed to send manual run signal."}), 500

@app.route('/api/config/update', methods=['POST'])
def api_config_update():
    """
    API endpoint to receive and update structured configuration settings.
    Validation is now relaxed to allow partial updates (e.g., only zone_properties).
    """
    try:
        updated_config_data = request.json
        # Normalize numeric values to floats after json parsing
        updated_config_data = _normalize_numbers_to_float(updated_config_data)
        
        # --- MODIFIED VALIDATION ---
        # Only check that the payload is not empty and is a dictionary
        if not updated_config_data or not isinstance(updated_config_data, dict):
             return jsonify({"success": False, "message": "Invalid configuration payload."}), 400
        # ---------------------------

        # The core merge logic is now handled inside write_structured_config(updated_config_data)
        if write_structured_config(updated_config_data):
            # After writing the file, signal the scheduler to reload the config
            reload_config_in_scheduler() 
            return jsonify({"success": True, "message": "Config updated and scheduler signaled to reload."}), 200
        else:
            return jsonify({"success": False, "message": "Failed to update configuration file."}), 500

    except Exception as e:
        logging.error(f"Error processing structured config update: {e}")
        return jsonify({"success": False, "message": f"Server error: {e}"}), 500
    
@app.route('/api/config/reload', methods=['POST'])
def api_config_reload():
    """API endpoint to force the scheduler to reload its configuration."""
    if reload_config_in_scheduler():
        return jsonify({"success": True, "message": "Config reload signal sent to scheduler."}), 200
    else:
        return jsonify({"success": False, "message": "Failed to send config reload signal."}), 500

@app.route('/api/status', methods=['GET'])
def api_status():
    """
    Returns the current scheduler status for AJAX polling.
    """
    full_status = full_status_prep(get_scheduler_status())
    return jsonify(full_status)

# This line is not executed when using Gunicorn, but useful for testing
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)