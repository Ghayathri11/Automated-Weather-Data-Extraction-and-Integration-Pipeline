import json
from pyowm.owm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps

import boto3
from datetime import datetime

def lambda_handler(event, context):
    owm = OWM('API Key')
    mgr = owm.weather_manager()
    observation = mgr.weather_at_place('India')
    w = observation.weather
    data={
    "country":"India",
    "status":w.detailed_status,
    "wind_speed":w.wind()['speed'],
    "humidity":w.humidity,
    "Pressure(Hg)":w.barometric_pressure(unit="inHg")['press'],
    "temperature(C)":w.temperature('celsius')['temp'],
    "max_temp":w.temperature('celsius')['temp_max'],
    "min_temp":w.temperature('celsius')['temp_min'],
    "sunrise":w.sunrise_time(timeformat='date').isoformat(),
    "sunset":w.sunset_time(timeformat='date').isoformat()
    }
    
    client=boto3.client('s3')
    filename="current_weather_india_"+str(datetime.now())+".json"
    client.put_object(
        Bucket="owm-etl-project",
        Key="transformed-data/"+filename,
        Body=json.dumps(data)

    )
