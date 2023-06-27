import os
import requests
import pandas as pd
import redshift_connector
from dotenv import load_dotenv
load_dotenv()

def country_extract(subregion):
    url = f"https://restcountries.com/v3.1/subregion/{subregion}?fields=name"

    countries = requests.get(url).json()
    pd_countries = pd.json_normalize(countries)
    countries = pd_countries['name.common']
    return countries.to_list()

def weather_extract(data):
    url = 'http://api.weatherapi.com/v1/current.json'
    try:
        weather = requests.get(url, params={'key': os.environ['API_KEY'], 'q': 'bulk'}, data= str(data)).json()
    except:
        raise Exception('Error in the request') 
    
    # Clean data and return it with a dataframe, returned only the relevant columns (location name, temperature, wind speed, wind direction, pressure, humidity, cloud, feels like, visibility, last updated)
    # weather = pd.json_normalize(weather['bulk'])

    return weather
