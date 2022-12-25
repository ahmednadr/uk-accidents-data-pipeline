def fetch_weather_by_year(long:float,lat:float,year:int):
    import requests 
    api_url = f"https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={long}&start_date={year}-01-01&end_date={year}-12-31&daily=temperature_2m_max&timezone=Europe%2FLondon"
    res = requests.get(api_url)
    assert(res.status_code == 200)
    return res.content

import pandas as pd
data = pd.read_json(fetch_weather_by_year(lat = 51.50,long = 13.12,year="2012")).daily
result  = pd.Series(data=data.temperature_2m_max,index=data.time)
print(result)

# day must be formated as yyyy-mm-dd
def fetch_weather_by_day(long:float,lat:float,day:str):
    import requests 
    api_url = f"https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={long}&start_date={day}&end_date={day}&daily=temperature_2m_max&timezone=Europe%2FLondon"
    res = requests.get(api_url)
    print(res.status_code)
    assert(res.status_code == 200)
    return res.content



# example for year's data
# data = pd.read_json(fetch_weather(51.50,-0.13,1991)).hourly
# result  = pd.Series(data=data.temperature_2m,index=data.time)
# print(result)

data = pd.read_json(fetch_weather_by_year(lat = 51.50,long = 13.12,year="2012")).daily
result  = pd.Series(data=data.temperature_2m_max,index=data.time)
print(result)