#!/bin/bash

# --- 1. Start the main scheduler (app.py) in the background ---
# The scheduler must run first to populate globals and set up the job.
# The '&' runs it in the background.
echo "Starting Heating Scheduler (app.py)..."
python /app/app.py --config /config/config.json &

# --- 2. Start the Flask GUI using Gunicorn in the foreground ---
# This keeps the container running.
# Assumption: Your Flask app instance is named 'app' inside 'web_gui.py'
echo "Starting Flask GUI with Gunicorn on 0.0.0.0:5000..."
exec gunicorn -w 4 -b 0.0.0.0:5000 web_gui:app