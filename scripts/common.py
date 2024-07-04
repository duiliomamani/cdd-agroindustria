import json
import os
import pandas as pd
import numpy as np


def read_csv(filename: str, sep: str = ","):
    df = pd.read_csv(filename, sep=sep)
    df = df.replace({np.nan: None})
    return df


def read_json(filename: str) -> dict | list:
    # read json from filename
    with open(filename, "r") as f:
        return json.load(f)


def write_json(filename: str, data: dict | list):
    dir_name = os.path.dirname(filename)

    # Create directory if it does not exist
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # write json to filename
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


def write_csv(filename: str, data: list[dict], sep: str = ","):
    dir_name = os.path.dirname(filename)

    # Create directory if it does not exist
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    df = pd.DataFrame(data)
    df.to_csv(filename, sep=sep, index=False)

