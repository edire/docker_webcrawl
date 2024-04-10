#%%

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
import io
import os
import dlogging
import datetime as dt
from dbharbor.sql import SQL

logger = dlogging.NewLogger(__file__, use_cd=True)

try:


    #%%

    logger.info('SQL Engine')
    engine_vars = {
        'db':os.getenv('sql_db'),
        'server':os.getenv('sql_server'),
        'uid':os.getenv('sql_uid'),
        'pwd':os.getenv('sql_pwd'),
    }

    engine = SQL(**engine_vars)


    #%%

    logger.info('Sharepoint Authentication')
    url = os.getenv('sharepoint_url')
    ctx_auth = AuthenticationContext(url)
    ctx_auth.acquire_token_for_app(os.getenv('sharepoint_client_id'), os.getenv('sharepoint_client_secret'))
    ctx = ClientContext(url, ctx_auth)


    #%%

    logger.info('Read Sales Tracker from Sharepoint')
    excel_file_path = os.getenv('excel_file_path_sales_tracker')

    response = File.open_binary(ctx, excel_file_path)
    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0)
    # bytes_file_obj.read()


    #%%

    logger.info('Verde SE Daily History')
    df = pd.read_excel(bytes_file_obj, sheet_name='Verde SE Sales Dollars', engine='openpyxl', skiprows=5, nrows=26)

    for col in df.columns:
        if not isinstance(col, str):
            pass
        elif col[:2] == 'DH':
            df_temp = df[['As of', col]].copy()
            df_temp = df_temp[~pd.isna(df_temp['As of'])]
            df_temp.reset_index(drop=True, inplace=True)
            df_temp.columns = ['descrip', 'value']
            df_temp['asofdate'] = dt.date.today() - dt.timedelta(days=1)

            logger.info(f'Upload to SQL staging - {col}')
            # engine.to_sql(df_temp, 'tblSalesTracker_DailyHistory_Verde', schema='stage', if_exists='replace', extras=True)
            df_temp.to_sql(f'tblSalesTracker_DailyHistory_Verde{col[2:]}', schema='stage', if_exists='replace', con=engine.con)

    engine.run('EXEC eggy.stpSalesTracker_DailyHistory_Verde')


    #%%

    logger.info('Sioux City Daily History')
    df = pd.read_excel(bytes_file_obj, sheet_name='Sioux City Sales Dollars', engine='openpyxl', skiprows=5, nrows=23)

    for col in df.columns:
        if not isinstance(col, str):
            pass
        elif col[:2] == 'DH':
            df_temp = df[['As of', col]].copy()
            df_temp = df_temp[~pd.isna(df_temp['As of'])]
            df_temp.reset_index(drop=True, inplace=True)
            df_temp.columns = ['descrip', 'value']
            df_temp['asofdate'] = dt.date.today() - dt.timedelta(days=1)

            logger.info(f'Upload to SQL staging - {col}')
    # engine.to_sql(df, 'tblSalesTracker_DailyHistory_SiouxCity', schema='stage', if_exists='replace', extras=True)
    df.to_sql(f'tblSalesTracker_DailyHistory_SiouxCity{col[2:]}', schema='stage', if_exists='replace', con=engine.con)

    engine.run('EXEC eggy.stpSalesTracker_DailyHistory_SiouxCity')


    #%%

    logger.info(f'Complete - {__name__}')


except Exception as e:
    logger.critical(str(e))
    raise