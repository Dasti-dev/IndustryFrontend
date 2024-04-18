import json
import pandas as pd
import numpy as np
from DataManager._influxdatabase import APIHandler

token = "FN1XbuLPfmUJ5xFZpjdI9m0TRq1jeNEcmh305vVV9nexhHo7FTwPHNC9NWKUP4yxvu2qzGL8UaAjykZUiZkejA=="
org = "self"
url = "http://localhost:8086/"

api = APIHandler(url, token, org)

# api.fetch_time_series_data(startTime, stopTime, resamplingFreq, listofsensor_id, listofsensor_data)

df = (api.fetch_time_series_data("2023-12-01", 
                                "2024-01-01", 
                                "1h", 
                                ['API_Gravity', 'Sulfur_Content_%', 'Viscosity_cP', 'Temperature_C', 'Pressure_bar', 'FlowRate_m3_hr'], 
                                ["CRUDE_AREA", "HYDRO_AREA", "UTILITIES_AREA"]))

for i in df.columns:
    df.rename(columns={i: f'{i[1]}_{i[0]}'}, inplace=True)

print(df)

df.index = pd.to_datetime(df.index)

df.reset_index(inplace=True)

df['Timestamp'] = pd.to_datetime(df['Timestamp'])

df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

json_data = df.to_json(orient='records')

# json_data = df.to_json(orient="index")

parsed_json = json.loads(json_data)
formatted_json = json.dumps(parsed_json, indent=4)

print(formatted_json)

with open(r'InfluxDB//data_fetched//CRUDE&HYDRO_AREA_data.json', 'w') as f:
            f.write(formatted_json)

colors = ['yellow', 'orange', 'red', 'green', 'blue', 'white']
np.random.seed(0)  # Seed for reproducibility

# Transforming the DataFrame into the structured JSON format
result = []
columns = df.columns[1:]  # Skip 'Timestamp' column

for column in columns:
    series_dict = {
        'id': column,
        'color': np.random.choice(colors),
        'data': [{'x': row['Timestamp'], 'y': row[column]} for index, row in df.iterrows()]
    }
    result.append(series_dict)

# Convert the result to JSON
formatted_json = json.dumps(result, indent=4)
print(formatted_json)

with open(r'InfluxDB//data_fetched//formatted_CRUDE&HYDRO_AREA_data.json', 'w') as f:
            f.write(formatted_json)
