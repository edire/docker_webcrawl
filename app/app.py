
import os
import dlogging
from demail.gmail import SendEmail


package_name = 'selenium_to_sql'
logger = dlogging.NewLogger(__file__, use_cd=True, backup_count=0)


#%% begin try

try:

    error_string = ''
    logger.info('Beginning package')


    #%% import libraries

    logger.info('Import Libraries')

    import pandas as pd
    import dwebdriver
    import ddb
    from ddb.sql import SQL
    from time import sleep
    
    directory = os.path.dirname(os.path.realpath(__file__))
    directory_download = os.path.join(directory, 'downloads')
    
    
    #%% odbc connector
    
    logger.info('Create ODBC connector')
    
    odbc = SQL(db=os.getenv('sql_db'), server=os.getenv('sql_server'), local_cred='no', uid=os.getenv('sql_uid'), pwd=os.getenv('sql_pwd'))
    
    
    #%% odbc env variables
    
    logger.info('Gather env dataframe')
    
    df_env = odbc.read('select * from eggy.tblScrapeDirector_EnvList')
    for s in range(df_env.shape[0]):
        os.environ[df_env.at[s, 'env_name']] = df_env.at[s, 'env_value']
        
    package_name = os.getenv('package_name')
    
    
    #%% odbc scrape director
    
    logger.info('Gather scraping dataframe')
    
    sql_scrape_director_tbl = os.getenv('sql_scrape_director_tbl')
    df_scrape = odbc.read(f'select * from {sql_scrape_director_tbl} order by sort_by')
    
   
    #%% Clean Workspace

    logger.info('Clean Workspace')
    
    for file in os.listdir(directory_download):
        filepath = os.path.join(directory_download, file)
        os.remove(filepath)
    
    
    #%% sql load
    
    logger.info('Define Functions')
    
    sql_schema_landing = os.getenv('sql_schema_landing')
    
    def SQLLoad(**kwargs):
        for file in os.listdir(directory_download):
            filepath = os.path.join(directory_download, file)
            filename, fileext = os.path.splitext(file)
            load_file = False
            if fileext == '.xlsx':
                df = pd.read_excel(filepath, **kwargs)
                load_file = True
            if load_file == True:
                df = ddb.clean(df, rowloadtime=True, drop_cols=False)
                sql_tbl = 'tbl' + ddb.clean_string(filename)
                odbc.to_sql(df, sql_tbl, schema=sql_schema_landing, if_exists='replace', index=False, extras=True)
            os.remove(filepath)
    
    
    #%% webscrape
    
    logger.info('Begin webscraping')
    
    with dwebdriver.ChromeDriver(download_directory=directory_download
                                  , no_sandbox=True
                                  , window_size='1920,1080'
                                  ) as driver:
    
        for i in range(df_scrape.shape[0]):
            df_temp = df_scrape[df_scrape.index == i].copy()
            logger.info(f"webscrape - {df_temp.at[i, 'descrip']}")
            if df_temp.at[i, 'command'] == 'url':
                try:
                    url = df_temp.at[i, 'command_value']
                    post_time_delay = df_temp.at[i, 'post_time_delay']
                    driver.get(url)
                    sleep(post_time_delay)
                except Exception as e:
                    e = str(e)
                    error_string += e + '\n\n'
                    logger.critical(e)
            elif df_temp.at[i, 'command'] == 'loadfile':
                try:
                    load_args = eval(df_temp.at[i, 'command_value'])
                    SQLLoad(**load_args)
                except Exception as e:
                    e = str(e)
                    error_string += e + '\n\n'
                    logger.critical(e)
            elif df_temp.at[i, 'command'] == 'runproc':
                try:
                    run_procs = df_temp.at[i, 'command_value']
                    for proc in run_procs.split(','):
                        sql = f'EXEC {proc}'
                        odbc.run(sql)
                except Exception as e:
                    e = str(e)
                    error_string += e + '\n\n'
                    logger.critical(e)
            else:
                try:
                    driver.process_df(df_temp, odbc_db=odbc)
                except Exception as e:
                    e = str(e)
                    error_string += e + '\n\n'
                    logger.critical(e)
                
    
    logger.info('Webscrape complete')
    
    
    #%% Success Email
    
    if error_string == '':
        sql = f"EXEC eggy.stpPythonLogging @package_name = '{package_name}', @is_success = 1, @error_descrip = NULL"
        odbc.run(sql)
        
        SendEmail(os.getenv('email_success'), subject=f'{package_name} Complete', body=f'Data successfully imported for {package_name}!', user=os.getenv('yagmail_uid'), password=os.getenv('yagmail_pwd'))
    
        logger.info('Done! No problems.\n')
        
    else:
        raise Exception("Errors occurred loading files\n\n")
    
    
#%% Error Handling

except Exception as e:
    e = str(e) + error_string
    logger.critical(f'{e}\n', exc_info=True)
    to_email_addresses = os.getenv('email_fail')
    body = f'Error running package {package_name}\nError message:\n{e}'
    SendEmail(to_email_addresses=to_email_addresses
                        , subject=f'Python Error - {package_name}'
                        , body=body
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('yagmail_uid')
                        , password=os.getenv('yagmail_pwd')
                        )

    sql = f"EXEC eggy.stpPythonLogging @package_name = '{package_name}', @is_success = 0, @error_descrip = '{e}'"
    odbc.run(sql)