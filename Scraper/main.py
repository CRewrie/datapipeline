import requests
import pandas as pd
from pymongo import MongoClient
import pymongo

pollution_api = "http://api.openweathermap.org/data/2.5/air_pollution"
weather_api = "https://api.openweathermap.org/data/2.5/weather"
mongo_URI = "mongodb://localhost:27017/"
client = MongoClient(mongo_URI)
mydb = client["citydata"]
mycol = mydb["pollutiondata"]
cities = pd.read_csv('/Users/constantinriewer/PycharmProjects/weather_scraper/cities.csv')

def pollutiondata(lat, lon):
    parameters = {
        "lat": lat,
        "lon": lon,
        "appid": "",
    }
    response = requests.get(pollution_api, params=parameters)
    response.raise_for_status()
    return response

def weatherdata(lat, lon):
    parameters = {
        "lat": lat,
        "lon": lon,
        "appid": "",
    }
    response = requests.get(weather_api, params=parameters)
    response.raise_for_status()
    return response

def main():
    with open('/Users/constantinriewer/PycharmProjects/weather_scraper/counter.txt') as f:
        batchid = int(f.readlines()[0])
    for index, row in cities.iterrows():
        lat = row["lat"]
        lon = row["long"]
        cityname = row["city"]
        wdata = weatherdata(lat, lon).json()
        pdata = pollutiondata(lat, lon).json()
        writetomongo = {"cityname": cityname, "batchid": batchid, "weatherdata": wdata, "polutiondata": pdata}
        x = mycol.insert_one(writetomongo)
    with open('/Users/constantinriewer/PycharmProjects/weather_scraper/counter.txt', 'w') as f:
        f.writelines(f"{batchid + 1}")

if __name__ == '__main__':
    main()



