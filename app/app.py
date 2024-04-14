#%% Template Imports

import os
import dlogging
from demail.gmail import SendEmail
import importlib
from prefect import task, flow
from dbharbor.sql import SQL


package_name = os.getenv('package_name')
logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


try:
    
    #%% engine connector

    logger.info('Create engine connector')

    engine_vars = {
        'db':os.getenv('sql_db'),
        'server':os.getenv('sql_server'),
        'uid':os.getenv('sql_uid'),
        'pwd':os.getenv('sql_pwd'),
    }

    engine = SQL(**engine_vars)


    #%% engine env variables

    logger.info('Gather env dataframe')

    engine_env_tbl = os.getenv('engine_env_tbl')
    df_env = engine.read(f'select * from {engine_env_tbl}')

    for s in range(df_env.shape[0]):
        os.environ[df_env.at[s, 'env_name']] = df_env.at[s, 'env_value']


    #%%

    @task
    def apx_webscrape():
        importlib.import_module('01_apx_webscrape')

    @task(retries=2, retry_delay_seconds=120)
    def sage_e130():
        importlib.import_module('02_get_sage_data_e130')

    @task(retries=2, retry_delay_seconds=120)
    def sage_e150():
        importlib.import_module('03_get_sage_data_e150')

    @task(retries=2, retry_delay_seconds=120)
    def sage_e160():
        importlib.import_module('03_get_sage_data_e160')

    @task
    def sharepoint_sales_daily_history():
        importlib.import_module('04_get_sharepoint_sales_daily_history')
    
    @task
    def get_sharepoint_commission():
        importlib.import_module('05_get_sharepoint_commission')

    @task
    def upload_sharepoint_commission():
        importlib.import_module('06_upload_sharepoint_commission')

    @task
    def upload_commission_summary():
        importlib.import_module('07_upload_commission_summaries')

    @task
    def romarket_cleanup():
        importlib.import_module('08_romarket_cleanup')

    @task
    def send_email():
        importlib.import_module('success_email')
        logger.info('Done! No problems.\n')


    @flow
    def pipeline():
        t1 = apx_webscrape.submit()
        t2 = sage_e130.submit()
        t3 = sage_e150.submit(wait_for=[t2])
        t31 = sage_e160.submit(wait_for=[t3])
        t4 = sharepoint_sales_daily_history.submit()
        t5 = get_sharepoint_commission.submit()
        t6 = upload_sharepoint_commission.submit(wait_for=[t1, t2, t3, t31, t5])
        t7 = upload_commission_summary.submit(wait_for=[t1, t2, t3, t31, t5])
        t8 = romarket_cleanup.submit(wait_for=[t1])
        send_email.submit(wait_for=[t1, t2, t3, t31, t4, t5, t6, t7, t8])

    pipeline()

except Exception as e:
    e = str(e)
    logger.critical(f'{e}\n', exc_info=True)
    SendEmail(to_email_addresses=os.getenv('email_fail')
                        , subject=f'Python Error - {package_name}'
                        , body=e
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('email_uid')
                        , password=os.getenv('email_pwd')
                        )