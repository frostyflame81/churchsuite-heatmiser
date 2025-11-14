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

# --------------------------------------------------------------------------
# 1. FLASK SETUP
# --------------------------------------------------------------------------
app = Flask(__name__)

# --------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# --------------------------------------------------------------------------

def get_env_config() -> Dict[str, Any]:
    """Retrieves relevant config variables from the environment."""
    # Filter the environment variables to only show relevant config items
    return {k: v for k, v in os.environ.items() if any(
        k.startswith(prefix) for prefix in ['CHURCHSUITE', 'OPENWEATHERMAP', 'PREHEAT', 'DEFAULT', 'ECO', 'NEOHUB']
    )}

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
    config_data = get_env_config()
    scheduler_pid = os.getppid()
    
    # NEW: Render the external template
    return render_template(
        'dashboard.html', # Reference the new file
        config=config_data,
        scheduler_pid=scheduler_pid
    )

@app.route('/api/trigger-manual', methods=['POST'])
def api_trigger_manual():
    """API endpoint to trigger a manual schedule update."""
    if trigger_manual_run_in_scheduler():
        return jsonify({"success": True, "message": "Manual run signal sent to scheduler."}), 200
    else:
        return jsonify({"success": False, "message": "Failed to send manual run signal."}), 500

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