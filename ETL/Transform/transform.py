import pandas as pd

def weather_json_to_df(weather):
    # Clean data and return it with a dataframe, returned only the relevant columns (location name, temperature, wind speed, wind direction, pressure, humidity, cloud, feels like, visibility, last updated)
    weather = pd.json_normalize(weather['bulk'])

    weather_df = weather[['query.q',
                          'query.location.name', 
                          'query.current.temp_c', 
                          'query.current.wind_kph', 
                          'query.current.wind_degree', 
                          'query.current.pressure_mb', 
                          'query.current.humidity', 
                          'query.current.cloud', 
                          'query.current.feelslike_c', 
                          'query.current.vis_km', 
                          'query.current.last_updated']].rename(columns={'query.q': 'country',
                                                                        'query.location.name': 'location_name',
                                                                        'query.current.temp_c': 'temperature',
                                                                        'query.current.wind_kph': 'wind_speed',
                                                                        'query.current.wind_degree': 'wind_direction',
                                                                        'query.current.pressure_mb': 'pressure',
                                                                        'query.current.humidity': 'humidity',
                                                                        'query.current.cloud': 'cloud',
                                                                        'query.current.feelslike_c': 'feels_like',
                                                                        'query.current.vis_km': 'visibility',
                                                                        'query.current.last_updated': 'last_updated'})
    
    return weather_df