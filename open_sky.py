import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os



keys = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", 
                    "latitude", "geo_altitude", "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors","baro_altitude", "squawk", "spi", "position_source", "category"]

basic = HTTPBasicAuth('jonas456',set.getenv('OPENSKYPASSWORD'))


def retrieve_data() :
    url = "https://opensky-network.org/api/states/all?lamin=48.3&lomin=1.5&lamax=49.2&lomax=3.5"
    try:
        response = requests.get(url, auth = basic,verify=False)
        response.raise_for_status()
        flights = response.json()
        values = flights["states"]
        # Get current UTC date and time formated as string
        flight_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+ 'Z'
        print(flight_time)
        #Recreate dictionnary
        info_flights = [{keys[i]: values[j][i] for i in range(len(values[j]))} for j in range(len(values))]
        print("Data collected suscessfuly! \n Number of flights: \n", len(info_flights))
        return info_flights,flight_time

    except Exception as e:
        print(e, response.txt)

retrieve_data()