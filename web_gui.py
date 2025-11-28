import os
import json
import logging
from flask import Flask, render_template, request, jsonify
import asyncio
from typing import Dict, Any

# Configure Flask logging
logging.basicConfig(level=logging.INFO)

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
        
    # 2. Ensure 'locations' key exists
    if 'locations' not in config:
         config['locations'] = {}
            
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
    
    # Keys in locations that must be floats
    LOCATION_FLOAT_KEYS = [
        "heat_loss_factor", 
        "min_external_temp"
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

    # --- 2. Process locations ---
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

    return config_data

def write_structured_config(config_data: Dict[str, Any]) -> bool:
    """Writes the updated configuration data back to config.json."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            # Use indent=4 for a human-readable config file
            json.dump(config_data, f, indent=4) 
        return True
    except Exception as e:
        logging.error(f"Failed to write structured config file: {e}")
        return False

def get_scheduler_status() -> Dict[str, Any]:
    """Reads the status file created by app.py."""
    try:
        with open(SCHEDULER_STATUS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"overall_status": "UNAVAILABLE", "last_run_time": "N/A", "neohub_reports": []}
    except Exception as e:
        logging.error(f"Error reading status file: {e}")
        return {"overall_status": "ERROR", "last_run_time": "N/A", "neohub_reports": []}
    
# Placeholder functions for interaction with the app.py process.
# IMPORTANT: Since app.py is a separate process, these stubs must be
# replaced with inter-process communication (IPC) logic (e.g., writing
# a flag file or using signals/Queues) to communicate with app.py's process.

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
    """Renders the main dashboard and config view."""
    config_data = get_structured_config()
    scheduler_pid = os.getppid()
    scheduler_status = get_scheduler_status()

    # NEW: Render the external template
    return render_template(
        'dashboard.html', # Reference the new file
        config=config_data,
        scheduler_pid=scheduler_pid,
        scheduler_status=scheduler_status
    )

@app.route('/api/trigger-manual', methods=['POST'])
def api_trigger_manual():
    """API endpoint to trigger a manual schedule update."""
    if trigger_manual_run_in_scheduler():
        return jsonify({"success": True, "message": "Manual run signal sent to scheduler."}), 200
    else:
        return jsonify({"success": False, "message": "Failed to send manual run signal."}), 500

@app.route('/api/config/update', methods=['POST'])
def api_config_update():
    """API endpoint to receive and update structured configuration settings."""
    try:
        updated_config_data = request.json
        # Normalize numeric values to floats after json parsing
        updated_config_data = _normalize_numbers_to_float(updated_config_data)
        # Basic validation: ensure the primary keys are present
        if not updated_config_data or 'global_settings' not in updated_config_data or 'locations' not in updated_config_data:
            return jsonify({"success": False, "message": "Invalid configuration payload. Missing global_settings or locations."}), 400

        # Write to the file located at /config/config.json
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

# This line is not executed when using Gunicorn, but useful for testing
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)