#%%

import os
import pandas as pd
from dbharbor.sql import SQL
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)
directory = os.path.dirname(__file__)
filepath_commissions = os.path.join(os.path.dirname(directory), 'templates', 'commissions.xlsx')

try:


    #%%

    logger.info('SQL Engine')
    engine_vars = {
        'db':os.getenv('sql_db'),
        'server':os.getenv('sql_server'),
        'uid':os.getenv('sql_uid'),
        'pwd':os.getenv('sql_pwd'),
    }

    con = SQL(**engine_vars)


    #%%

    logger.info('Get Commission Proc Input Data')
    df = con.read('EXEC dbo.stpCommission_Input')

    logger.info('Load Workbook New')
    with pd.ExcelWriter(filepath_commissions, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, index=False, header=False, sheet_name='New', startrow=3)


    #%%

    logger.info('Get Commission Add Input Data')
    df = con.read('SET NOCOUNT ON SELECT * FROM dbo.vCommissionRates')

    logger.info('Load Workbook History')
    with pd.ExcelWriter(filepath_commissions, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, index=False, header=False, sheet_name='History', startrow=3)


    #%%

    logger.info('Sharepoint Authentication')
    url = os.getenv('sharepoint_url')
    ctx_auth = AuthenticationContext(url)
    ctx_auth.acquire_token_for_app(os.getenv('sharepoint_client_id'), os.getenv('sharepoint_client_secret'))
    ctx = ClientContext(url, ctx_auth)


    #%%

    logger.info('Upload to sharepoint')
    target_folder = ctx.web.get_folder_by_server_relative_url(os.getenv('sharepoint_upload_folder') + '/Leadership')

    with open(filepath_commissions, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file('Commission_Approvals.xlsx', file_content).execute_query()


    #%%

    logger.info(f'Complete - {__name__}')


except Exception as e:
    logger.critical(str(e))
    raise