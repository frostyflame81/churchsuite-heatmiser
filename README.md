# **ChurchSuite Heatmiser Integration**

## **Status**

This project is currently at stable release **v1.0.0.0**. It has evolved from a simple automation script into a robust service featuring a dedicated web management interface and an advanced thermal calculation engine. After releasing this version, further development will switch to a development branch.

## **Purpose**

This application automates Heatmiser neo heating systems based on ChurchSuite bookings. It fetches booking data and intelligently adjusts the heating schedule, ensuring rooms are warm exactly when needed while minimizing energy waste.

The core of this integration is its **Dynamic Thermal Model**, which calculates preheat times based on building insulation, external weather forecasts, and residual heat from previous events—vastly outperforming standard "fixed" preheat methods.

## **Features**

* **Automated Scheduling:** Syncs ChurchSuite venue bookings directly to Heatmiser profiles.  
* **Web Dashboard (GUI):** A user-friendly interface to monitor system health, edit configurations, and view logs.  
* **Advanced Preheat Engine:** Uses exponential decay (Newton's Law of Cooling) and weather compensation to determine the perfect start time.  
* **Multi-Hub Support:** Control multiple NeoHubs across different physical sites from a single instance.  
* **Log Export:** Granular logging with a built-in export tool for troubleshooting.  
* **Docker Ready:** Simplified deployment using Docker and Docker Compose.

## **Configuration Guide (config.json)**

The configuration controls the "intelligence" of the system. These values can be edited via the **Config** tab in the Web GUI.

### **Global Settings**

These variables adjust the baseline logic for all zones.

| Variable | Default | Logic & Adjustment |
| :---- | :---- | :---- |
| PREHEAT\_TIME\_MINUTES | 30.0 | **Fixed Offset.** The minimum time added to every booking. Increase if the system generally struggles to reach temperature even in mild weather. |
| DEFAULT\_TEMPERATURE | 18.0 | **Occupied Temp.** The target temperature for the duration of a booking. |
| ECO\_TEMPERATURE | 12.0 | **Setback Temp.** The temperature maintained when a room is vacant. |
| HEAT\_LOSS\_MULTIPLIER\_MAX | 1.46 | **Weather Compensation.** The maximum scaling factor for preheat during extreme cold. (e.g., 1.5 would increase a 60min preheat to 90min). |
| TEMP\_WARM\_THRESHOLD | 12.0 | External temp above which no extra "cold weather" scaling is applied. |
| TEMP\_COLD\_THRESHOLD | \-5.0 | External temp at which the HEAT\_LOSS\_MULTIPLIER\_MAX is fully applied. |

### **Advanced Thermal Variables**

* **HEAT\_LOSS\_CONSTANT**: (Defined per Hub) This represents the building's thermal mass. A higher value (e.g., 2000\) is for modern, well-insulated buildings. A lower value (e.g., 800\) is for older, drafty stone churches.  
* **heat\_loss\_factor**: (Defined per Location) A multiplier for specific zones. If a specific room (like a high-ceilinged hall) takes longer to heat than others on the same hub, increase this value (e.g., 1.5).  
* **min\_external\_temp**: (Optional) A safety cutoff. If the external temperature is above this value, the system assumes the building hasn't cooled enough to require the full preheat calculation.

## **The Web Dashboard (GUI)**

The GUI is accessible at http://\<your-ip\>:5000.

### **Current Functionality**

1. **Dashboard:** Real-time view of the scheduler status. It displays which NeoHubs are online, which locations are currently "Active" (heating), and a summary of the most recent integration run.  
2. **Config Management:** No more manual JSON editing. The Config tab allows you to update global settings and map ChurchSuite locations to specific Heatmiser zones. If you don’t have a config file on first run, the system will generate one for you.  
3. **Log Viewer & Export:** View live application logs. You can filter logs by severity (INFO, DEBUG, ERROR) and export them as a .log file directly to your computer for remote support.  
4. **Manual Overrides:** Trigger an immediate sync with ChurchSuite or force the scheduler to reload its configuration after making changes.

### **Future Roadmap: The Setup Wizard**

We are currently developing an **Initial Setup Wizard** to further simplify deployment. Planned features include:

* **Auto-Discovery:** Polling the network to identify NeoHubs automatically.  
* **Source Validation:** Testing the ChurchSuite API feed and OpenWeatherMap keys during setup.  
* **Interactive Mapping:** A drag-and-drop interface to link your ChurchSuite "Rooms" to your Heatmiser "Zones" based on data polled directly from the devices.

## Important Notes on Network Configuration

* **Server Location:** The server running this application must be located on the same local network as the Heatmiser neo hub(s) *unless* you have configured network address translation (NAT) and port forwarding.  
* **Remote Heatmiser neo Hubs:** If you need to control Heatmiser neo hubs on a remote network (e.g., in a separate building), you will need to:  
  * Configure your router at the remote location to forward port 4243 (the default Heatmiser neo port) to the internal IP address of the Heatmiser neo hub.  
  * Use a Dynamic DNS service (e.g., DuckDNS, No-IP) to provide a stable hostname for the remote network's public IP address.  
  * You will also need your heatmiser system to be using a 7-day program with 6 comfort zones, the software will fail if this configuration is not present. Please note, changing this setting on your heatmiser app will delete all current programs so please be aware.

## **Installation**

### **1\. Prerequisites**

* Docker and Docker Compose installed.  
* An API key from [OpenWeatherMap](https://openweathermap.org/).  
* IP address or hostname and API key from each Heatmiser NeoHub that you wish to control (generated from the mobile app).  
* Your ChurchSuite JSON feed URL.

### **2\. Clone the repository**

Clone the repository to your local machine:

```
git clone https://github.com/frostyflame81/churchsuite-heatmiser
cd churchsuite-heatmiser
```

### **3\. Deployment**

Rename docker-compose.sample to docker-compose.yml. The following environment variables need to be set. The easiest way to do this is with a .env file in the project root, or directly in the docker-compose.yml

* OPENWEATHERMAP\_API\_KEY: Your OpenWeatherMap API key. Get one from [https://openweathermap.org/api](https://openweathermap.org/api).  
* OPENWEATHERMAP\_CITY: The city for which to retrieve weather data (e.g., "London").  
* CHURCHSUITE\_URL: The URL of the ChurchSuite JSON feed for bookings and locations.  
* NEOHUB\_ADDRESS\_MAIN: The IP address or hostname of your primary Heatmiser neo hub. If this is on a remote network, this should be the Dynamic DNS address.  
* NEOHUB\_API\_KEY\_MAIN: The API key for your primary Heatmiser neo hub.  
* NEOHUB\_ADDRESS\_CHURCH\_HALL: The IP address or hostname of your secondary Heatmiser neo hub. If this is on a remote network, this should be the Dynamic DNS address.  
* NEOHUB\_API\_KEY\_CHURCH\_HALL: The API key for your secondary Heatmiser neo hub.

Once the environment is configured, launch run:

docker-compose up \-d

### **3\. Initial Config**

After the container is running, visit the Web GUI to set up your config.json via the browser. Or manually set it up with the editor of your choice.

## **License**

This project is licensed under the **GPLv3 License**.