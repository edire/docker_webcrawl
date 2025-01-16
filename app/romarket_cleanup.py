#%%

import os
import pandas as pd
from dbharbor.sql import SQL
import datetime as dt
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)


#%%

con = SQL(
    server = os.getenv('sql_server'),
    db = os.getenv('sql_db'),
    uid = os.getenv('sql_uid'),
    pwd = os.getenv('sql_pwd'),
)


#%%

metrics = [
    'Booked Total',
    'Budget',
    'Variance to Budget',
    'Inventory',
    'Occupied',
    'Available',
    'Rate to Meet Budget',
    'Current Avg Rate',
    'Avg Rate YTD',
    'Occupancy %',
    'Prior Year',
    'Total',
    'Variance to PY',
    'Inventory',
    'Occupied',
    'Available',
    'PY Avg Rate',
    'PY Avg Rate YTD',
    'PY Occupancy %',
    'Prior Year MTD',
    'Variance to PY MTD',
    'CY as % of PY MTD',
    'PY Occupied MTD',
]

def get_location(x):
    if pd.isna(x) == False and x not in metrics:
        return x
    else:
        return None
    
def monthend(x):
    mnth, yr = x.split(r'/')
    mnth = int(mnth)
    yr = 2000 + int(yr)
    dte = dt.date(yr, mnth, 1) + pd.tseries.offsets.MonthEnd(0)
    return dte

def con_float(x):
    if pd.isna(x) == False:
        if '%' in x:
            x = x.replace('%', '')
            x = x.replace(',', '')
            x = float(x)
            x = x / 100
        elif x == '-':
            x = 0
        else:
            x = x.replace(',', '')
            x = float(x)
    return x


def clean_and_load(sql_name):
    df = con.read(f'select * from stage.tbl{sql_name}')
    df['location'] = df[' '].apply(get_location)
    df['location'] = df['location'].ffill()
    df = df[~pd.isna(df[' '])]
    df = df[df[' '] != df['location']]
    df = df.drop(columns=[f'IDtbl{sql_name}', 'RowLoadDateTime'])
    df['dups'] = df.groupby(['location', ' ']).cumcount()
    df['dups'] = ['' if x == 0 else f'_{str(x)}' for x in df['dups']]
    df[' '] = df[' '] + df['dups']
    df.drop(columns='dups', inplace=True)

    df = pd.melt(df, id_vars=['location', ' '], var_name='dte', value_name='amount')
    df = df[[r'/' in x for x in df['dte']]]
    df['dte'] = df['dte'].apply(monthend)
    df.rename({' ':'metric'}, axis=1, inplace=True)
    df['amount'] = df['amount'].apply(con_float)

    con.to_sql(df, f'tbl{sql_name}_pivot', schema='stage', if_exists='replace', index=False)
    con.run(f'exec eggy.stp{sql_name}_pivot')


#%%

clean_and_load('ROMarketH')
logger.info(f'Complete - {__name__}')


#%%