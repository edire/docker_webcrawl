#%% Imports

import os
import pandas as pd
import json
import dbharbor
from dbharbor.sql import SQL
import dwebdriver
import dlogging
import tempfile
import shutil


logger = dlogging.NewLogger(__file__, use_cd=True)


#%% engine connector

engine_vars = {
    'db':os.getenv('sql_db'),
    'server':os.getenv('sql_server'),
    'uid':os.getenv('sql_uid'),
    'pwd':os.getenv('sql_pwd'),
}

engine = SQL(**engine_vars)


#%% Functions

def _SQLLoad(temp_dir, **kwargs):
    i = 0
    for file in os.listdir(temp_dir):
        i += 1
        filepath = os.path.join(temp_dir, file)
        filename, fileext = os.path.splitext(file)
        load_file = False

        if fileext == '.xlsx':
            df = pd.read_excel(filepath, **kwargs['load_args'])
            load_file = True
        elif fileext == '.csv':
            df = pd.read_csv(filepath)
            load_file = True

        if load_file == True:
            if 'clean' in kwargs:
                if kwargs['clean'] == 0:
                    pass
            else:
                df = dbharbor.clean(df, rowloadtime=True, drop_cols=False)
            if 'sql_tbl' not in kwargs:
                sql_tbl = 'tbl' + dbharbor.clean_string(filename)
                sql_schema_landing = 'stage'
            elif '.' in kwargs['sql_tbl']:
                sql_schema_landing, sql_tbl = kwargs['sql_tbl'].split('.')
            else:
                sql_tbl = kwargs['sql_tbl']
                sql_schema_landing = None
            engine.to_sql(df, sql_tbl, schema=sql_schema_landing, if_exists='replace', index=False, extras=True)
        shutil.rmtree(temp_dir)
    if i == 0:
        raise Exception(f"No downloaded files found, kwargs: {kwargs}")
    

def GetWebDriver():
    temp_dir = tempfile.mkdtemp()
    driver = dwebdriver.ChromeDriver(
        download_directory=temp_dir,
        no_sandbox=True,
        window_size='1920,1080',
        use_chromium = True,
        headless=False
    )
    return driver, temp_dir


def WebCrawlProcess(driver, df_scrape, temp_dir=None):
    for i in range(df_scrape.shape[0]):
        df_temp = df_scrape[df_scrape.index == i].copy()
        logger.info(df_temp.at[i, 'command_name'])
        if df_temp.at[i, 'command'] == 'loadfile':
            command_value_type = df_temp.at[i, 'command_value_type']
            command_value = df_temp.at[i, 'command_value']
            if command_value_type == 'python':
                _locals = {}
                exec(command_value, None, _locals)
            _SQLLoad(temp_dir, **_locals)
        elif df_temp.at[i, 'command'] == 'runproc':
            run_procs = df_temp.at[i, 'command_value']
            for proc in run_procs.split(','):
                engine.run(proc)
        else:
            driver.process_df(df_temp, odbc_db=engine)
            

def CrawlPath(crawl_list):
    df_crawl = pd.DataFrame()
    for file in crawl_list:
        with open(file) as f:
            data = json.load(f)
        df_temp = pd.DataFrame(data).T.reset_index(names=['command_name'])
        df_crawl = pd.concat([df_crawl, df_temp], axis=0)
    df_crawl.reset_index(inplace=True, drop=True)
    return df_crawl


