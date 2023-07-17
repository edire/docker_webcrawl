#%%

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
import io
import os
import dlogging
from ddb.sql import SQL
import ddb

logger = dlogging.NewLogger(__file__, use_cd=True)


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

logger.info('Read Commission from Sharepoint')
excel_file_path = os.getenv('excel_file_path_commission')

response = File.open_binary(ctx, excel_file_path)
bytes_file_obj = io.BytesIO()
bytes_file_obj.write(response.content)
bytes_file_obj.seek(0)
# bytes_file_obj.read()


#%%

logger.info('Get New DataFrame')
df = pd.read_excel(bytes_file_obj, sheet_name='New', engine='openpyxl', skiprows=2)
df = df[['yes' in x.lower() for x in df['Review Complete? (Yes/No)']]]

if not df.empty:
    columns = ['Customer ID', 'Customer Name', 'Contract ID', 'AE #1', 'AE #1 Commission %', 'AE #2', 'AE #2 Commission %', 'Contract Class']
    df = df[columns]

    logger.info('Upload to SQL staging')
    df = ddb.clean(df, drop_cols=False)
    df.to_sql('tblCommission_Approval', schema='stage', if_exists='replace', con=engine.con)

    logger.info('Run SQL update')
    engine.run('EXEC dbo.stpCommissionRates')

else:
    logger.info('No records to load')


#%%

logger.info('Get History DataFrame')
df = pd.read_excel(bytes_file_obj, sheet_name='History', engine='openpyxl', skiprows=2)
df = df[['yes' in x.lower() for x in df['Update Record? (Yes/No)']]]

if not df.empty:
    columns = ['Customer ID', 'Customer Name', 'Contract ID', 'AE #1', 'AE #1 Commission %', 'AE #2', 'AE #2 Commission %', 'Contract Class', 'Start Date']
    df = df[columns]

    logger.info('Upload to SQL staging')
    df = ddb.clean(df, drop_cols=False)
    df.to_sql('tblCommission_Approval_Add', schema='stage', if_exists='replace', con=engine.con)

    logger.info('Run SQL update')
    engine.run('EXEC dbo.stpCommissionRates_Add')

else:
    logger.info('No records to update')


#%%

logger.info('Sharepoint Reads Complete')


# %%
