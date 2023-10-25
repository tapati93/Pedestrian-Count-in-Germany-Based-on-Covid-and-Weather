import imp
import requests
import mysql.connector
import maya

mydb = mysql.connector.connect(
	host = "bab1tbn9rvwqgxjhbahc-mysql.services.clever-cloud.com",
	user = "uonjbsvxl0hehkqn",
	password = "QOOlPOyvR48HX1wu99py",
	database = "bab1tbn9rvwqgxjhbahc"
)

mycursor = mydb.cursor()


url = "https://static.hystreet.com/api/https://hystreet.com/api/locations"

headers = {
    "Content-Type": "application/json",
    "X-API-Token": "nACLQT4zJDABKCMm8gWfayop"
}

try:    
## Call to API 1 - To get today's pedestrian count of all 194 locations
 AllCities_Response =  requests.get(url,headers=headers).json()
 print("AllCities_Response  ", AllCities_Response)
 
 
## Call to API 2 - To get today's pedestrian count, weather and temperature of each city

 city_information = []
 for i in range(len(AllCities_Response)):
 
    city_number = i
    each_city_response = AllCities_Response[i]
    city_id = each_city_response.get('id')
    print("city_id  ",city_id)
    each_city_url = "https://static.hystreet.com/api/https://hystreet.com/api/locations/"+str(city_id)

    each_city_response = requests.get(each_city_url, headers=headers).json()
    print("each_city_response  ",each_city_response)

    city_name = each_city_response.get('city')

    street_name = each_city_response.get('name')
    print(street_name)

    pedestriancount_today = each_city_response.get(('statistics'))
    pedestriancount_today = pedestriancount_today.get('today_count')
    print(pedestriancount_today)

    measurements = each_city_response.get('measurements')
    print(measurements)
    weathercondition_today = [ sub['weather_condition'] for sub in measurements ]
    weathercondition_today = ''.join(weathercondition_today)
    print(weathercondition_today)

    temperature = [ sub['temperature'] for sub in measurements ]
    temperature = ''.join(str(e) for e in temperature)

    print(temperature)

    min_temperature_today = [ sub['min_temperature'] for sub in measurements ]
    min_temperature_today = ''.join(str(e) for e in min_temperature_today)
    print(min_temperature_today)

    print("val", (city_name, street_name,pedestriancount_today,weathercondition_today, temperature, min_temperature_today))

    sql = "INSERT INTO Pedestrian_Data_Today (city_name, street_name, pedestriancount_today,weathercondition_today, tempearture, min_temperature_today) VALUES (%s, %s,%s, %s,%s, %s)"
    val = (city_name, street_name,pedestriancount_today,weathercondition_today, temperature, min_temperature_today)
    mycursor.execute(sql, val)

    mydb.commit()
    print(mycursor.rowcount, "record inserted into Pedestrian_Data_Today.")
    
## Call to API 3 - To get past four years pedestrian count, weather and temperature of each city
    querystring = {"from":"2019-05-02T00:","to":"2022-05-02T00:","resolution":"day"}

    each_city_historical_response = requests.get( each_city_url, headers=headers, params=querystring).json()
    print("each_city_historical_response   ",each_city_historical_response)
    
    city_name_past =  each_city_historical_response.get('city')
    street_name_past = each_city_historical_response.get('name')
    
    measurements_past = each_city_historical_response.get('measurements')
    for i in range(len(measurements_past)):
      measure = measurements_past[i]
      print(measure)
      pedestriancount_past = measure.get('pedestrians_count')
      print(pedestriancount_past)
      weathercondition_past= measure.get('weather_condition')
      temperature_past=  measure.get('temperature')
      min_temperature_past = measure.get('min_temperature')
      date_past=  measure.get('timestamp')
      date_past = maya.parse(date_past).datetime()

      sql1 = "INSERT INTO Pedestrian_Data_Past (city_name, street_name, pedestriancount,weathercondition, tempearture, min_temperature,date) VALUES (%s, %s,%s, %s,%s, %s, %s)"
      val1 = (city_name_past, street_name_past,pedestriancount_past,weathercondition_past, temperature_past, min_temperature_past,date_past)
      mycursor.execute(sql1, val1)

      mydb.commit()
      print(mycursor.rowcount, "record inserted into Pedestrian_Data_Past.")

except Exception as e:
     print("An exception occurred ",e)




