import pandas as pd
import time
import requests
import calendar
import json

with open('api_key.txt') as f:
    querystring = json.loads(f.read())

headers = {
    'cache-control': "no-cache"
    }
    
min_year = 1920
max_year = 2022
min_month = 5
max_month = 10

columns = ["fecha","indicativo","nombre","provincia","altitud","tmed","prec","tmin","tmax","horatmin","horatmax"]
working_df = pd.DataFrame(columns=columns)

def download_data_from_aemet(querystring, headers, min_year, max_year, min_month, max_month, working_df):
    for year in range(min_year, max_year + 1):
        print(year)
        for month in range(min_month, max_month + 1):
            day_range = calendar.monthrange(year, month)
            if len(str(month)) == 1:
                url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{year}-0{month}-01T10%3A00%3A00UTC/fechafin/{year}-0{month}-{day_range[1]}T10%3A00%3A00UTC/todasestaciones"
            else:
                url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{year}-{month}-01T10%3A00%3A00UTC/fechafin/{year}-{month}-{day_range[1]}T10%3A00%3A00UTC/todasestaciones"
            
            time.sleep(5)
            response = requests.request("GET", url, headers=headers, params=querystring)
            link = json.loads(response.text)['datos']
            f = requests.get(link)
            data = f.json()
            data_df = pd.json_normalize(data)
            working_df = pd.concat([working_df, data_df[working_df.columns.intersection(data_df.columns)]])
            
def download_csv(working_df):
    file_name = '../aemet_data.csv'
    working_df.to_csv(file_name, sep='\t', encoding='utf-8')
   
    
if __name__ == "__main__":
    download_data_from_aemet(querystring, headers, min_year, max_year, min_month, max_month, working_df)
    download_csv(working_df)