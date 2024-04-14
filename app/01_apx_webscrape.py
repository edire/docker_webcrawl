#%% Imports

import os
import dlogging
import pandas as pd
import dwebdriver
import dbharbor
from dbharbor.sql import SQL


package_name = os.getenv('package_name')
logger = dlogging.NewLogger(__file__, use_cd=True, backup_count=0)
directory = os.path.dirname(os.path.realpath(__file__))
directory_download = os.path.join(directory, 'downloads')

error_string = ''
logger.info('Beginning package')


#%% engine connector

logger.info('Create engine connector')

engine_vars = {
    'db':os.getenv('sql_db'),
    'server':os.getenv('sql_server'),
    'uid':os.getenv('sql_uid'),
    'pwd':os.getenv('sql_pwd'),
}

engine = SQL(**engine_vars)


#%% engine scrape director

logger.info('Gather scraping dataframe')

filepath_scrapedirector = os.getenv('dev_scrape_director', '')
if os.path.exists(filepath_scrapedirector):
    df_scrape = pd.read_csv(filepath_scrapedirector)
    df_scrape = df_scrape[pd.isna(df_scrape['command'])==False]
    df_scrape.reset_index(drop=True, inplace=True)
else:
    sql_scrape_director_tbl = os.getenv('sql_scrape_director_tbl')
    df_scrape = engine.read(f'select * from eggy.tblScrapeDirector_APX order by sort_by')


#%% Clean Workspace

logger.info('Clean Workspace')

for file in os.listdir(directory_download):
    filepath = os.path.join(directory_download, file)
    os.remove(filepath)


#%% sql load

logger.info('Define Functions')

def SQLLoad(**kwargs):
    i = 0
    for file in os.listdir(directory_download):
        if file != '.gitkeep':
            i += 1
        logger.info(file)
        filepath = os.path.join(directory_download, file)
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
        os.remove(filepath)
    if i == 0:
        raise Exception(f"No downloaded files found, kwargs: {kwargs}")


#%% webscrape

logger.info('Begin webscraping')

with dwebdriver.ChromeDriver(download_directory=directory_download
                                , no_sandbox=True
                                , window_size='1920,1080'
                                , use_chromium = True
                                ) as driver:

    for i in range(df_scrape.shape[0]):
        df_temp = df_scrape[df_scrape.index == i].copy()
        logger.info(f"webscrape - {df_temp.at[i, 'descrip']}")
        if df_temp.at[i, 'command'] == 'loadfile':
            try:
                command_value_type = df_temp.at[i, 'command_value_type']
                command_value = df_temp.at[i, 'command_value']
                if command_value_type == 'python':
                    _locals = {}
                    exec(command_value, None, _locals)
                SQLLoad(**_locals)
            except Exception as e:
                e = str(e)
                error_string += e + '\n\n'
                logger.critical(e)
        elif df_temp.at[i, 'command'] == 'runproc':
            try:
                run_procs = df_temp.at[i, 'command_value']
                for proc in run_procs.split(','):
                    engine.run(proc)
            except Exception as e:
                e = str(e)
                error_string += e + '\n\n'
                logger.critical(e)
        else:
            try:
                driver.process_df(df_temp, odbc_db=engine)
            except Exception as e:
                e = str(e)
                error_string += e + '\n\n'
                logger.critical(e)
            

logger.info('Webscrape complete')


#%% Success Email

if error_string == '':
    logger.info('Record success in SQL and send email')

    sql = f"INSERT INTO {os.getenv('error_logging_tbl')} (package_name, is_success, error_descrip)" \
        f"\nVALUES ('{package_name}', 1, NULL)"
    engine.run(sql)

    logger.info(f'Complete - {__name__}')
    
else:
    logger.info('Record errors in SQL and raise Exception')

    e = error_string.replace("'", "")
    sql = f"INSERT INTO {os.getenv('error_logging_tbl')} (package_name, is_success, error_descrip)" \
        f"\nVALUES ('{package_name}', 0, '{e}')"
    engine.run(sql)

    raise Exception('Errors occured during processing!\n\n' + error_string)