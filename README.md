# ChurchSuite Heatmiser Integration

## Status
This product is pre-alpha - it currently compiles and runs with errors - this is the bug fix phase to actually get it working!

## Purpose

This application automates the control of Heatmiser neo heating systems based on bookings in ChurchSuite. It fetches booking data from ChurchSuite and adjusts the heating schedule for your venue, ensuring rooms are heated only when needed.

## Features

* **Automated Heating Control:** Automatically adjusts heating based on ChurchSuite bookings.
* **Multi-Zone Support:** Supports multiple Heatmiser neo zones.
* **Pre-Heating:** Calculates and applies pre-heating time, adjustable based on external temperature.
* **External Temperature Adjustment:** Optionally adjusts pre-heating time based on external temperature to optimize energy usage.
* **Docker Deployment:** Designed to be deployed using Docker for easy setup and consistent operation.
* **JSON Data:** Fetches data from ChurchSuite using JSON feeds.
* **Websocket Communication:** Communicates with Heatmiser neo systems using Websockets.
* **GPLv3 Licence:** This project is licenced under the GPLv3 licence.

## Prerequisites

Before you begin, ensure you have the following installed and configured:

* **Docker:** This application uses Docker to run in a container. Install Docker on your system. See the official Docker documentation for installation instructions:
    * [Get Docker](https://docs.docker.com/get-docker/)
* **Python:** Python 3.9 or later is required (if not using Docker).
* **Git:** Git is required to clone the repository.

## Important Notes on Network Configuration

* **Server Location:** The server running this application must be located on the same local network as the Heatmiser neo hub(s) *unless* you have configured network address translation (NAT) and port forwarding.
* **Remote Heatmiser neo Hubs:** If you need to control Heatmiser neo hubs on a remote network (e.g., in a separate building), you will need to:
    * Configure your router at the remote location to forward port 4243 (the default Heatmiser neo port) to the internal IP address of the Heatmiser neo hub.
    * Use a Dynamic DNS service (e.g., DuckDNS, No-IP) to provide a stable hostname for the remote network's public IP address.
    * You will also need your heatmiser system to be using a 7-day program with 6 comfort zones, the software will fail if this configuration is not present. Please note, changing this setting on your heatmiser app will delete all current programs so please be aware.

## Installation and Setup

Follow these steps to get the application running:

### 1. Clone the Repository

Clone the repository to your local machine:

```
git clone https://github.com/frostyflame81/churchsuite-heatmiser
cd churchsuite-heatmiser
```

### 2. Prepare the Configuration

* The repository already contains a `config` directory and a sample `config.json`, it will need to be manually edited before you start the app.
* Your `config.json` should look something like this:

    ```json
    {
        "locations": {
            "Location Name in ChurchSuite": {
                "neohub": "neohub_name",
                "zones": ["Zone Name in Heatmiser"],
                "heat_loss_factor": 1.5,
                "min_external_temp": 7
            }
        },
        "preheat_time_minutes": 30,
        "default_temperature": 20,
        "eco_temperature": 16,
        "temperature_sensitivity": 10,
        "preheat_adjustment_minutes_per_degree": 5
    }
    ```
* Edit the `config.json` file:
    * **`locations`**: A dictionary mapping ChurchSuite location names to Heatmiser neo hub names and zone names.
        * `neohub`: The name you will use to refer to this Heatmiser neo hub in the `docker-compose.yml` file.
        * `zones`: A list of Heatmiser neo zone names that correspond to this ChurchSuite location.
        * `heat_loss_factor`: an adjustment to compenstate pre-heat time based on heat loss.
        * `min_external_temp`: a location specific value, used together with temperature sensitivity.
    * **`preheat_time_minutes`**: The default time in minutes to start heating a venue before a booking.
    * **`default_temperature`**: The target temperature in degrees Celsius when a room is occupied.
    * **`eco_temperature`**: The temperature to set when a room is not occupied.
    * `temperature_sensitivity`: The outside temperature below which the preheat adjustment applies.
    * `preheat_adjustment_minutes_per_degree`: Minutes added to preheat per degree below sensitivity, multiplied by heat\_loss\_factor.

### 3. Set up Environment Variables

The following environment variables need to be set. The easiest way to do this is with a `.env` file in the project root, or directly in the `docker-compose.yml`

* `OPENWEATHERMAP_API_KEY`: Your OpenWeatherMap API key. Get one from <https://openweathermap.org/api>.
* `OPENWEATHERMAP_CITY`: The city for which to retrieve weather data (e.g., "London").
* `CHURCHSUITE_URL`: The URL of the ChurchSuite JSON feed for bookings and locations.
* `NEOHUB_ADDRESS_MAIN`: The IP address or hostname of your primary Heatmiser neo hub. If this is on a remote network, this should be the Dynamic DNS address.
* `NEOHUB_API_KEY_MAIN`: The API key for your primary Heatmiser neo hub.
* `NEOHUB_ADDRESS_CHURCH_HALL`: The IP address or hostname of your secondary Heatmiser neo hub. If this is on a remote network, this should be the Dynamic DNS address.
* `NEOHUB_API_KEY_CHURCH_HALL`: The API key for your secondary Heatmiser neo hub.

### 4. `docker-compose.yml`

Use the included `docker-compose.yml` file or generate your own based on the example below:

```yaml
version: '3.8'
services:
  web:
    build: .
    ports:
      - "5000:5000"
    volumes:
      - .:/app
      - ./config:/config
    environment:
      - OPENWEATHERMAP_API_KEY=${OPENWEATHERMAP_API_KEY}
      - OPENWEATHERMAP_CITY=${OPENWEATHERMAP_CITY}
      - CHURCHSUITE_URL=${CHURCHSUITE_URL}
      - NEOHUB_ADDRESS_MAIN=${NEOHUB_ADDRESS_MAIN}
      - NEOHUB_API_KEY_MAIN=${NEOHUB_API_KEY_MAIN}
      - NEOHUB_ADDRESS_CHURCH_HALL=${NEOHUB_ADDRESS_CHURCH_HALL}
      - NEOHUB_API_KEY_CHURCH_HALL=${NEOHUB_API_KEY_CHURCH_HALL}
    restart: unless-stopped
```

### 5. Run with Docker Compose

Start the application using Docker Compose:

```
docker-compose up -d
```

This will build the Docker image and start the container in detached mode. The application will now run in the background, automatically adjusting your heating schedule based on ChurchSuite bookings.
