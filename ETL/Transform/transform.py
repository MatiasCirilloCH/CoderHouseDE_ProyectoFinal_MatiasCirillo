import pandas as pd

# function to change wind direction from degrees to cardinal direction (N, S, E, W)
def wind_direction(degrees):
    if degrees > 337.5 or degrees < 22.5:
        return 'N'
    elif degrees > 22.5 and degrees < 67.5:
        return 'NE'
    elif degrees > 67.5 and degrees < 112.5:
        return 'E'
    elif degrees > 112.5 and degrees < 157.5:
        return 'SE'
    elif degrees > 157.5 and degrees < 202.5:
        return 'S'
    elif degrees > 202.5 and degrees < 247.5:
        return 'SW'
    elif degrees > 247.5 and degrees < 292.5:
        return 'W'
    elif degrees > 292.5 and degrees < 337.5:
        return 'NW'

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
    
    weather_df['last_updated'] = pd.to_datetime(weather_df['last_updated'])

    # change wind direction from degrees to cardinal direction (N, S, E, W)
    weather_df['wind_direction'] = weather_df['wind_direction'].apply(lambda x: wind_direction(x))

    return weather_df