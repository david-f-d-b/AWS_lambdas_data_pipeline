import json

import json
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import requests 



def lambda_handler(event, context):
  print(event)
  schema=
  host=
  user="admin"
  password=
  port=3306
  con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
  engine = create_engine(con)
  #cities = ["Berlin", "Paris", "London"]
  df_weather = weather_details()  # Get the weather data DataFrame

  table_weather = 'weather_test'
  tries = 0
    
  #while status_code != 200 & num_tries < 10:
  try:
        df_weather.to_sql(table_weather, con=engine, if_exists="append", index=False)
        print("success !")
        #num_tries+=1
  except Exception as e:
        print(f"error : {e}")
  
  return {
      'statusCode': 200,
      'body': json.dumps('Hello from Lambda!')
  }

def weather_details():
    API_key = "155ac89acb627b07f1c68cb640aef942"
    schema="gans"
    host="wbs-project-db.chkzj5qwklyl.eu-north-1.rds.amazonaws.com"
    user="admin"
    password="dgH7{y,mQ"
    port=3306
    con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    engine = create_engine(con)
    # Fetch city_id data from the 'city_id' table in the 'gans' schema
    city_id_query = "SELECT city_name, id FROM gans.city_id"
    city_id = pd.read_sql(city_id_query, con=engine)  # Use your existing database connection 'engine'
    cities = city_id['city_name'].to_list()
    
    # Creating a list to store weather data
    weather_data = []

    # Loop through each city and fetch weather data
    for city in cities:
        weather_url = (f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_key}&units=metric")
        weather_response = requests.get(weather_url)
        weather_json = weather_response.json()

        # Getting the information from the JSON
        for i in weather_json["list"]:
            city_id_value = city_id.loc[city_id['city_name'] == city, 'id'].values[0]
            
            weather_entry = {
                "city_id": city_id_value,
                "city": weather_json["city"]["name"],
                "country": weather_json["city"]["country"],
                "forecast_time": i["dt_txt"],
                "temperature": i["main"]["temp"],
                "temperature_feels_like": i["main"]["feels_like"],
                "outlook": i["weather"][0]["main"],
                "outlook_description": i["weather"][0]["description"],
                "wind_speed": i["wind"]["speed"],
                "wind_deg": i["wind"]["deg"],
                "pressure": i["main"]["pressure"],
                "humidity": i["main"]["humidity"],
                "clouds": i.get("clouds", {}).get("all", "0"),
                "snow": i.get("snow", {}).get("all", "0"),
                "rain": i.get("rain", {}).get("3h", "0")
            }
            weather_data.append(weather_entry)

    # Create a DataFrame from the weather data
    weather_df = pd.DataFrame(weather_data)
    
    return weather_df




