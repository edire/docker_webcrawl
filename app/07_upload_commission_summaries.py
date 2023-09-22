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
from openpyxl.styles import NamedStyle


logger = dlogging.NewLogger(__file__, use_cd=True)
directory = os.path.dirname(__file__)
filepath_commissions = os.path.join(directory, 'commission_summary.xlsx')
filepath_commissions_ae = os.path.join(directory, 'commission_summary_ae.xlsx')


error_string = ''
year = (dt.datetime.now() - dt.timedelta(days=3)).year
currency_style = NamedStyle(name='custom_01', number_format='_(\$* #,##0_);_(\$* (#,##0);_(\$* -??_);_(@_)')
percent_style = NamedStyle(name='custom_02', number_format='0.0%')


#%% Sharepoint Connect

logger.info('Sharepoint Authentication')
url = os.getenv('sharepoint_url')
ctx_auth = AuthenticationContext(url)
ctx_auth.acquire_token_for_app(os.getenv('sharepoint_client_id'), os.getenv('sharepoint_client_secret'))
ctx = ClientContext(url, ctx_auth)


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
    workbook = writer.book
    worksheet = writer.sheets['Detail']
    for cell in worksheet['D']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['E']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['F']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['G']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['O']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['P']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['Q']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['R']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['S']:
        if isinstance(cell.value, (int, float)):
            cell.style = percent_style
    for cell in worksheet['T']:
        if isinstance(cell.value, (int, float)):
            cell.style = percent_style
    for cell in worksheet['U']:
        if isinstance(cell.value, (int, float)):
            cell.style = percent_style
    for cell in worksheet['V']:
        if isinstance(cell.value, (int, float)):
            cell.style = percent_style

logger.info('Upload to sharepoint')

try:
    target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder'))
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

df_ae_list = con.read("SELECT * FROM dbo.vAEList")

for ae in df_ae_list['ae']:

    logger.info(f'Get Commission Proc Summary Data for {ae}')
    df_ae = con.read(f"EXEC dbo.stpCommissionSummary '{ae}'")

    logger.info(f'Get Commission Proc Approved Rates for {ae}')
    df_ae_rates = con.read(f"EXEC dbo.stpCommissionRates_AE '{ae}'")

    if df_ae.empty == False:
        filepath_temp = os.path.join(directory, f'commission_summary_{ae}.xlsx')
        shutil.copy2(filepath_commissions_ae, filepath_temp)

        logger.info('Load Workbook')
        df_ae_name = pd.DataFrame([ae], columns=['ae_name'])
        with pd.ExcelWriter(filepath_temp, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            df_ae.to_excel(writer, index=False, header=False, sheet_name='Detail', startrow=4)
            df_ae_name.to_excel(writer, index=False, header=False, sheet_name='Verde Outdoor Commissions', startrow=1, startcol=2)
            df_ae_rates.to_excel(writer, index=False, header=False, sheet_name='Approved Commission Rates', startrow=4)

            workbook = writer.book

            worksheet = writer.sheets['Detail']
            for cell in worksheet['D']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['E']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['F']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['G']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['O']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['P']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['Q']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['R']:
                if isinstance(cell.value, (int, float)):
                    cell.style = currency_style
            for cell in worksheet['S']:
                if isinstance(cell.value, (int, float)):
                    cell.style = percent_style

            worksheet = writer.sheets['Approved Commission Rates']
            for cell in worksheet['G']:
                if isinstance(cell.value, (int, float)):
                    cell.style = percent_style


        logger.info('Upload to sharepoint')
        try:
            office = df_ae['Office'].iat[0]
            target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder'))
            ae_folder = target_folder.folders.add(office)
            ctx.execute_query()
            target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder') + f'/{office}')
            ae_folder = target_folder.folders.add(ae)
            ctx.execute_query()
            with open(filepath_temp, 'rb') as content_file:
                file_content = content_file.read()
                ae_folder.upload_file(f'Commission_Summary_{year}_{ae}.xlsx', file_content).execute_query()

            logger.info(f'Upload complete for {ae}!')
        except Exception as e:
            e = str(e)
            logger.critical(e)
            error_string += e + '\n\n'

        os.remove(filepath_temp)
    else:
        logger.info('No data to load')

logger.info('All uploads complete!')


#%% Upload Invoice Errors

logger.info('Get Invoice Issues Proc Data')
df_iss = con.read('EXEC dbo.stpCommissionInvoiceIssues')

filepath_issues = os.path.join(directory, 'invoice_issues.xlsx')
filepath_temp = os.path.join(directory, 'invoice_issues_temp.xlsx')
shutil.copy2(filepath_issues, filepath_temp)


logger.info('Load Workbook New')
with pd.ExcelWriter(filepath_temp, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    df_iss.to_excel(writer, index=False, header=False, sheet_name='Sheet1', startrow=1)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    for cell in worksheet['E']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['F']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style
    for cell in worksheet['K']:
        if isinstance(cell.value, (int, float)):
            cell.style = currency_style

logger.info('Upload to sharepoint')
try:
    target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder') + '/Leadership')
    with open(filepath_temp, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file(f'Invoice_Issues.xlsx', file_content).execute_query()
    logger.info('Upload complete for Invoice Issues!')

except Exception as e:
    e = str(e)
    logger.critical(e)
    error_string += e + '\n\n'

os.remove(filepath_temp)


#%%

if error_string == '':
    logger.info('All Commission Summary Uploads Complete!')
    
else:
    logger.info('Some Commission Summary Uploads Failed!')

    raise Exception('Errors occured during processing!\n\n' + error_string)


#%%