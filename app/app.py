#%% Template Imports

import os
import dlogging
from demail.gmail import SendEmail
import importlib
from prefect import task, flow


package_name = os.getenv('package_name')
logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


#%%

try:

    #%%

    @task
    def get_apx_webcrawl():
        importlib.import_module('get_apx_webcrawl')

    @task
    def get_sage_data():
        importlib.import_module('get_sage_data')

    @task
    def get_sharepoint_sales_daily_history():
        importlib.import_module('get_sharepoint_sales_daily_history')
    
    @task
    def get_sharepoint_commission():
        importlib.import_module('get_sharepoint_commission')

    @task
    def upload_sharepoint_commission():
        importlib.import_module('upload_sharepoint_commission')

    @task
    def upload_commission_summaries():
        importlib.import_module('upload_commission_summaries')

    @task
    def send_email():
        importlib.import_module('success_email')
        logger.info('Done! No problems.\n')


    @flow
    def pipeline():
        t_apx = get_apx_webcrawl.submit()
        t_sage = get_sage_data.submit()
        t_daily_history = get_sharepoint_sales_daily_history.submit()
        t_get_commission = get_sharepoint_commission.submit()
        t_put_commission = upload_sharepoint_commission.submit(wait_for=[t_apx, t_sage, t_get_commission])
        t_put_commission_summaries = upload_commission_summaries.submit(wait_for=[t_apx, t_sage, t_get_commission])
        send_email.submit(wait_for=[
            t_apx,
            t_sage,
            t_daily_history,
            t_get_commission,
            t_put_commission,
            t_put_commission_summaries
            ])

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
    

#%%