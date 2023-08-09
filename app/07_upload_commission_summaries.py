#%%

import os
import shutil
import numpy as np
import pandas as pd
from ddb.sql import SQL
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import dlogging
import datetime as dt


error_string = ''
year = (dt.datetime.now() - dt.timedelta(days=3)).year


logger = dlogging.NewLogger(__file__, use_cd=True)
directory = os.path.dirname(__file__)
filepath_commissions = os.path.join(directory, 'commission_summary.xlsx')


#%% Sharepoint Connect

logger.info('Sharepoint Authentication')
url = os.getenv('sharepoint_url')
ctx_auth = AuthenticationContext(url)
ctx_auth.acquire_token_for_app(os.getenv('sharepoint_client_id'), os.getenv('sharepoint_client_secret'))
ctx = ClientContext(url, ctx_auth)

target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder'))

#%% SQL Connect

logger.info('SQL Connection')
con = SQL()


#%% Upload Main Summary

logger.info('Get Commission Proc Summary Data')
df = con.read('EXEC dbo.stpCommissionSummary NULL')

filepath_temp = os.path.join(directory, 'commission_summary_temp.xlsx')
shutil.copy2(filepath_commissions, filepath_temp)

logger.info('Load Workbook Commission Summary')
with pd.ExcelWriter(filepath_temp, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    df.to_excel(writer, index=False, header=False, sheet_name='Detail', startrow=4)

logger.info('Upload to sharepoint')

try:
    with open(filepath_temp, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file(f'Commission_Summary_{year}.xlsx', file_content).execute_query()
    logger.info('Upload complete for Main Summary!')

except Exception as e:
    e = str(e)
    logger.critical(e)
    error_string += e + '\n\n'

os.remove(filepath_temp)


#%% Upload individual Summaries

logger.info('Begin running individual AE reports')
unique_ae_list = np.append(
    df['ae_1'],
    df['ae_2']
)
unique_ae_list = unique_ae_list[~pd.isna(unique_ae_list)]
unique_ae_list = np.unique(unique_ae_list)

for ae in unique_ae_list:

    logger.info(f'Get Commission Proc Summary Data for {ae}')
    df_ae = con.read(f"EXEC dbo.stpCommissionSummary '{ae}'")

    filepath_temp = os.path.join(directory, f'commission_summary_{ae}.xlsx')
    shutil.copy2(filepath_commissions, filepath_temp)

    logger.info('Load Workbook')
    with pd.ExcelWriter(filepath_temp, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        df_ae.to_excel(writer, index=False, header=False, sheet_name='Detail', startrow=4)

    logger.info('Upload to sharepoint')
    try:
        with open(filepath_temp, 'rb') as content_file:
            file_content = content_file.read()
            target_folder.upload_file(f'Commission_Summary_{year}_{ae}.xlsx', file_content).execute_query()

        logger.info(f'Upload complete for {ae}!')
    except Exception as e:
        e = str(e)
        logger.critical(e)
        error_string += e + '\n\n'

    os.remove(filepath_temp)

logger.info('All uploads complete!')


#%% Upload Invoice Errors

logger.info('Get Invoice Issues Proc Data')
df_iss = con.read('EXEC dbo.stpCommissionInvoiceIssues')

filepath_issues = os.path.join(directory, 'invoice_issues.xlsx')

logger.info('Load Workbook New')
with pd.ExcelWriter(filepath_issues, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    df_iss.to_excel(writer, index=False, header=False, sheet_name='Sheet1', startrow=1)

logger.info('Upload to sharepoint')
try:
    with open(filepath_issues, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file(f'Invoice_Issues.xlsx', file_content).execute_query()
    logger.info('Upload complete for Invoice Issues!')

except Exception as e:
    e = str(e)
    logger.critical(e)
    error_string += e + '\n\n'


#%%

if error_string == '':
    logger.info('All Commission Summary Uploads Complete!')
    
else:
    logger.info('Some Commission Summary Uploads Failed!')

    raise Exception('Errors occured during processing!\n\n' + error_string)


#%%