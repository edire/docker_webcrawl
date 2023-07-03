#%% Template Imports

import os
import dlogging
from demail.gmail import SendEmail
import importlib


package_name = os.getenv('package_name')
logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


try:

    importlib.import_module('01_apx_webscrape')
    importlib.import_module('02_get_sage_data_e130')
    importlib.import_module('03_get_sage_data_e150')
    importlib.import_module('04_get_sharepoint_sales_daily_history')
    importlib.import_module('05_get_sharepoint_commission')
    importlib.import_module('06_upload_sharepoint_commission')
    importlib.import_module('success_email')

    logger.info('Done! No problems.\n')


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