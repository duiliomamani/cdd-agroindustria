import os
import time
from datetime import datetime
import requests
import common

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = f"{ROOT_DIR}/input"
OUTPUT_DIR = f"{ROOT_DIR}/output"
API_ROUTE = "https://archive-api.open-meteo.com/v1/"

def get_data_weather_forecast(departent_id: str, lat: float, lon: float):
    try:
        year_start = 1969
        year_end = 2023

        start_date = f"{year_start}-01-01"
        end_date = f"{year_end}-12-31"
        measure = "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,precipitation_hours"
        url = f"{API_ROUTE}/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&timezone=auto&daily={measure}"
        response = requests.get(url)
        data = response.json()

        # if response.status_code == 429:
        #     print(response.text)
        #     if data["reason"] == "Hourly API request limit exceeded. Please try again in the next hour.":
        #         time.sleep(3600)
        #     if data["reason"] == "Daily API request limit exceeded. Please try again tomorrow.":
        #         time.sleep(600)
        #     else:
        #         time.sleep(120)
        #     get_data_weather_forecast(departent_id, lat, lon)

        weather_data = {}
        for i in range(len(data["daily"]["time"])):
            date = datetime.strptime(data["daily"]["time"][i], "%Y-%m-%d")

            weather_data_row = {
                "date": data["daily"]["time"][i],
                "temperature_2m_max": data["daily"]["temperature_2m_max"][i],
                "temperature_2m_min": data["daily"]["temperature_2m_min"][i],
                "temperature_2m_mean": data["daily"]["temperature_2m_mean"][i],
                "precipitation_sum": data["daily"]["precipitation_sum"][i],
                "rain_sum": data["daily"]["rain_sum"][i],
                "precipitation_hours": data["daily"]["precipitation_hours"][i],
            }
            # Agrupar por mes
            if date.year not in weather_data:
                weather_data[date.year] = {}
            if str(date.month).zfill(2) not in weather_data[date.year]:
                weather_data[date.year][str(date.month).zfill(2)] = []

            weather_data[date.year][str(date.month).zfill(2)].append(weather_data_row)

        department_weather = {
            "id_departamento": departent_id,
            "coords": f"{lat},{lon}",
            "weather": weather_data,
        }
        return department_weather
    
    except Exception as e:
        return {"error": True, "message": str(e)}
    
# ----------- Ejecución del script
if __name__ == "__main__":
    result_weather = []
    data_process = common.read_csv(f"{INPUT_DIR}/departamentos.csv")
    dpto_process = []
    for (i, item) in enumerate(data_process):
        print("Processing", i, "of", len(data_process))            
        print("Processing", {item['id_departamento_normalized']}-{item['departamento']} )
        weather = get_data_weather_forecast(
            item["id_departamento_normalized"],
            item["departamento_lat"],
            item["departamento_lon"],
        )
        if "error" in weather and weather["error"] == True:
            dpto_process.append(f"{item['id_departamento_normalized']}-{item['departamento']}-'error")
            print("Error", {item['id_departamento_normalized']}-{item['departamento']} )
        else:
            dpto_process.append(f"{item['id_departamento_normalized']}-{item['departamento']}-'success")
             
        common.write_csv(f"{OUTPUT_DIR}/data_departamentos_process.csv", dpto_process)
        common.write_json(f"{OUTPUT_DIR}/weather/{item['provincia_iso_id']}_{item['id_departamento_normalized']}.json", weather)
        time.sleep(180)