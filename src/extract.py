import requests as rq
import json
from db_config import API_KEY

def extract_data():

    #parameters for Rome
    LAT = 41.9028
    LON = 12.4964

    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&units=metric&exclude=minutely,alerts&appid={API_KEY}"

    response = rq.get(url)
    data = response.json()
    
    # Save raw data to file for downstream tasks
    with open("raw_data.json", "w") as f:
        json.dump(data, f)