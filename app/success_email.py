#%%

import os
from demail.gmail import SendEmail
from ddb.sql import SQL
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)


#%%

logger.info('SQL Connector')
engine_vars = {
    'db':os.getenv('sql_db'),
    'server':os.getenv('sql_server'),
    'uid':os.getenv('sql_uid'),
    'pwd':os.getenv('sql_pwd'),
}

engine = SQL(**engine_vars)


#%%

logger.info('Get last sale date')

sql_last_sale = "SELECT MAX(sr.SignedDate) as max_sale FROM dbo.tblSalesReport sr"
df_last_sale = engine.read(sql_last_sale)
str_last_sale = f'Last sale date loaded to SQL: {str(df_last_sale.iat[0, 0])}'


#%%

logger.info('Send Email')

SendEmail(to_email_addresses=os.getenv('email_success'),
            subject=f'{os.getenv("package_name")} Complete',
            body=f'Data successfully imported for {os.getenv("package_name")}!\n\n{str_last_sale}',
            user=os.getenv('email_uid'),
            password=os.getenv('email_pwd'))

logger.info('Email Sent Successfully')