#%%

import os
import requests
import pandas as pd
from time import sleep
import json
from dbharbor.sql import SQL
import dbharbor
import dlogging

os.chdir(os.path.dirname(__file__))
logger = dlogging.NewLogger(__file__, use_cd=True)
APX_API_TOKEN = os.getenv('APX_API_TOKEN')
headers={"Authorization": f"Bearer {APX_API_TOKEN}"}
params = {'page_size': 1000}


#%%

logger.info('Connecting to SQL')
engine_vars = {
    'db':os.getenv('sql_db'),
    'server':os.getenv('sql_server'),
    'uid':os.getenv('sql_uid'),
    'pwd':os.getenv('sql_pwd'),
}

engine = SQL(**engine_vars)


#%%

# response = requests.request("GET", base_url, headers=headers, params=params)
# df = pd.DataFrame(response.json()['data'])

logger.info('Defining get_all_results function')
def get_all_results(base_url, headers, params):
    df = pd.DataFrame()
    page = 1
    s = 1
    while True:
        current_params = {**params, 'page': page}
        response = requests.get(base_url, headers=headers, params=current_params)
        if response.status_code == 429:
            if s >= 60:
                raise Exception('Too many APX rate limits, killing process')
            logger.info('Rate limited, waiting 30 seconds')
            sleep(30)
            s += 1
        else:
            response.raise_for_status()
            data = response.json()['data']
            if not data:
                break
            df = pd.concat([df, pd.DataFrame(data)], axis=0)
            page += 1
    return df


#%%

logger.info('Reading API list')
with open('./apx_api.json', 'r') as f:
    api_list = json.load(f)

logger.info('Iterating through API list')
for api in api_list:
    logger.info(f'Getting data for {api}')
    base_url = api_list[api]
    df = get_all_results(base_url, headers, params)
    df = dbharbor.clean(df, rowloadtime=True, drop_cols=False)
    logger.info(f'Loading data for {api}')
    engine.to_sql(df, name=f'tbl{api}', schema='apx', if_exists='replace', index=False)


logger.info('Done')


#%%