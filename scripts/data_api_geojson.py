import os
import common

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = f"{ROOT_DIR}/input"
OUTPUT_DIR = f"{ROOT_DIR}/output"
API_ROUTE = "https://apis.datos.gob.ar/georef/api"

def get_data_province(id_province: str):
    # url = f"{API_ROUTE}/provincias?id={str(id_province).zfill(2)}"
    # response = requests.get(url)
    # return response.json()
    provincies = common.read_json(f"{INPUT_DIR}/provincias.json")["provincias"]
    id_pr = str(id_province).zfill(2)
    return [item for item in provincies if item["id"] == id_pr][0]

def get_data_departament(id_province: str, id_departament: str):
    # url = f"{API_ROUTE}/departamentos?id={str(id_province).zfill(2)}{str(id_departament).zfill(3)}"
    # response = requests.get(url)
    # return response.json()
    deptos = common.read_json(f"{INPUT_DIR}/departamentos.json")["departamentos"]
    id_dpto = str(id_province).zfill(2) + str(id_departament).zfill(3)
    dpto = [item for item in deptos if item["id"] == id_dpto]
    return dpto[0] if dpto else {"id": id_dpto, "nombre_completo": "Sin Definir", "centroide": {"lat": 0 ,"lon": 0}}


def get_data(row: dict):
    id_province = row["id_provincia"]
    id_departament = row["id_departamento"]
    province = get_data_province(id_province)
    departament = get_data_departament(id_province, id_departament)
    row["id_provincia_normalized"] = str(province["id"])
    row["provincia_iso_id"] = province["iso_id"]
    row["provincia_lat"] = str(province["centroide"]["lat"])
    row["provincia_lon"] = str(province["centroide"]["lon"])
    row["id_departamento_normalized"] = str(departament["id"])
    row["departamento_normalized"] = departament["nombre_completo"]
    row["departamento_lat"] = str(departament["centroide"]["lat"])
    row["departamento_lon"] = str(departament["centroide"]["lon"])
    return row

# ----------- Ejecución del script
if __name__ == "__main__":

    data = common.read_csv(f"{INPUT_DIR}/data_agro_v0.csv")

    index = 0
    resulut = []
    for item in data:
         row = get_data(item)
         row["index"] = index
         resulut.append(row)
         index += 1
         print(f"Processing {index} of {len(data)} data")

    common.write_csv(f"{OUTPUT_DIR}/data_agro_v1.csv", resulut)

    