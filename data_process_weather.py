import concurrent.futures
import os
import common
import itertools

def thread_method(call_method: callable, data, max_workers: int = 2) -> list[dict]:
    index = 0
    data_result = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(call_method, item) for item in data]
        for future in concurrent.futures.as_completed(futures):
            index += 1
            data_result.append(future.result())

    executor.shutdown()
    return data_result

def _process_weather(row: dict):
    # file_path = f"{row['provincia_iso_id']}_{row['id_departamento_normalized']}_{row['departamento']}.json"
    # meteo = meteo_dict[file_path] if not("Sin Definir" in file_path) else None
    if 'Sin Definir' in row['departamento']:
        meteo = None
    else:
        meteo = next((d for d in meteo_dict if d['id_departamento'] == row['id_departamento_normalized']), None)
    
    year_start = row['campana'][:4]
    year_end = row['campana'][5:]
    cultive = cultives_dict[row['cultivo']]
    # Detail Campain
    row['inicio_campana'] = f"{cultive['sowing']}/{year_start if cultive['same_year'] else year_end}"
    row['fin_campana'] = f"{cultive['harvest']}/{year_start if cultive['same_year'] else year_end}"
    
    if meteo is None:
         # Prom
        row['prom_temperature_2m_max'] = None
        row['prom_temperature_2m_min'] = None 
        row['prom_temperature_2m_mean'] = None
        row['prom_precipitation_sum'] = None 
        row['prom_rain_sum'] = None
        row['prom_precipitation_hours'] = None
        # Total
        row['sum_rain_total'] = None 
        row['sum_precipitation_total'] = None
        row['sum_precipitation_hours_total'] = None
        
        return row
    
    month_campana = []
    
    if cultive['same_year']:
        month_campana = [meteo['weather'][year_start][str(index).zfill(2)] for index in range(int(cultive['sowing']), int(cultive['harvest']) + 1)]
    else:
        month_campana = [meteo['weather'][year_start][str(index).zfill(2)] for index in range(int(cultive['sowing']), 13)]
        month_campana += [meteo['weather'][year_end][str(index).zfill(2)] for index in range(1, int(cultive['harvest']) + 1)]
        
    dailys = list(itertools.chain(*month_campana))
    sum_temperature_2m_max = sum(value['temperature_2m_max'] for value in dailys)
    sum_temperature_2m_min = sum(value['temperature_2m_min'] for value in dailys)
    sum_temperature_2m_mean = sum(value['temperature_2m_mean'] for value in dailys)
    sum_precipitation_total = sum(value['precipitation_sum'] for value in dailys)
    sum_rain_total = sum(value['rain_sum'] for value in dailys)
    sum_precipitation_hours_total = sum(value['precipitation_hours'] for value in dailys)
    total_dailys = len(dailys)
    # Prom
    row['prom_temperature_2m_max'] = round(sum_temperature_2m_max / total_dailys, 2)
    row['prom_temperature_2m_min'] = round(sum_temperature_2m_min/ total_dailys, 2)
    row['prom_temperature_2m_mean'] = round(sum_temperature_2m_mean / total_dailys, 2)
    row['prom_precipitation_sum'] = round(sum_precipitation_total / total_dailys, 2)
    row['prom_rain_sum'] = round(sum_rain_total / total_dailys, 2)
    row['prom_precipitation_hours'] = round(sum_precipitation_hours_total / total_dailys, 2)
    # Total
    row['sum_rain_total'] = round(sum_rain_total, 2)
    row['sum_precipitation_total'] = round(sum_precipitation_total, 2)
    row['sum_precipitation_hours_total'] = round(sum_precipitation_hours_total, 2)

    return row

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = f"{ROOT_DIR}/input"
OUTPUT_DIR = f"{ROOT_DIR}/output"

cultives = common.read_csv(f"{INPUT_DIR}/cultivos.csv")
cultives_dict = {c['name']: c for c in cultives.to_dict("records")}

data_agro = common.read_csv(f'{INPUT_DIR}/data_agro_v1.csv')

meteo_dict = {}

files = os.listdir(OUTPUT_DIR + "/weather")
files = [OUTPUT_DIR + "/weather/" + file for file in files]
meteo_dict = thread_method(common.read_json, files, max_workers=50)

data_agro_new = thread_method(_process_weather, data_agro.to_dict("records"), max_workers=50)
    
common.write_csv(f'{OUTPUT_DIR}/data_agro.csv', data_agro_new)