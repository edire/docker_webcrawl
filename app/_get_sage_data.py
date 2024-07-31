#%%

import os
import pandas as pd
from sageintacctsdk import SageIntacctSDK
import datetime as dt
from dbharbor.sql import SQL

import dlogging
logger = dlogging.NewLogger(__file__, use_cd=True)
lookback_date = dt.date.today() + dt.timedelta(days=-60)
lookback_date = lookback_date.strftime(r'%m/%d/%Y')


#%%

con = SQL(
    db = os.getenv('sql_db'),
    server = os.getenv('sql_server'),
    uid = os.getenv('sql_uid'),
    pwd = os.getenv('sql_pwd')
    )


#%% Functions

def GetSageConnection(company_id):
    sage_con = SageIntacctSDK(
        sender_id = os.getenv('sender_id'),
        sender_password = os.getenv('sender_password'),
        user_id = os.getenv('user_id'),
        company_id = company_id,
        user_password = os.getenv('user_password')
    )
    return sage_con


def ARInvoices(sage_con, suffix):

    fields = [
        'RECORDNO',
        'RECORDID',
        'STATE',
        'CUSTOMERID',
        'CUSTOMERNAME',
        'WHENCREATED',
        'WHENPOSTED',
        'WHENDISCOUNT',
        'WHENDUE',
        'WHENPAID',
        'TOTALENTERED',
        'TOTALSELECTED',
        'TOTALPAID',
        'TOTALDUE',
        'WHENMODIFIED',
    ]

    query_tuple_greaterthan = [('greaterthan', 'WHENMODIFIED', lookback_date)]
    response = sage_con.ar_invoices.get_by_query(fields=fields, and_filter=query_tuple_greaterthan)
    # response = connection.ar_invoices.get_by_query(fields=fields)
    # response = connection.ar_invoices.get_all()
    df = pd.DataFrame(response)

    logger.info(f'tblARInvoices{suffix}')
    con.to_sql(df, f'tblARInvoices{suffix}', schema='stage', if_exists='replace', index=False)

    con.run("EXEC eggy.stpARInvoices")


def CustomerData(sage_con, suffix):

    fields = [
        'RECORDNO',
        'CUSTOMERID',
        'NAME',
        'ENTITY',
        'PARENTKEY',
        'PARENTID',
        'PARENTNAME',
        'CUSTREPNAME',
        'STATUS',
        'WHENCREATED',
    ]

    response = sage_con.customers.get_by_query(fields=fields)
    df = pd.DataFrame(response)

    con.to_sql(df, f'tblCustomers{suffix}', schema='stage', if_exists='replace', index=False)

    con.run("EXEC eggy.stpCustomers")


#%% Payment Details

def PaymentDetails(sage_con):

    fields = [
        'RECORDNO',
        'RECORDKEY',
        'PAYMENTKEY',
        'PAYMENTENTRYKEY',
        'PAYMENTDATE',
        'PAYMENTAMOUNT',
        'WHENCREATED',
        'WHENMODIFIED',
        'CREATEDBY',
        'MODIFIEDBY',
    ]

    query_tuple_greaterthan = [('greaterthan', 'WHENMODIFIED', lookback_date)]
    response = sage_con.ar_payment_detail.get_by_query(fields=fields, and_filter=query_tuple_greaterthan)
    # response = connection.ar_payment_detail.get_by_query(fields=fields)
    df = pd.DataFrame(response)

    con.to_sql(df, 'tblARPaymentDetails', schema='stage', if_exists='replace', index=False)

    con.run("EXEC eggy.stpARPaymentDetails")


#%% Payments


# fields = [
#     'RECORDNO',
#     'PRBATCHKEY',
#     'CUSTOMERID',
#     'CUSTOMERNAME',
#     'WHENCREATED',
#     'RECEIPTDATE',
#     'WHENPAID',
#     'TOTALENTERED',
#     'TOTALPAID',
#     'MEGAENTITYID',
# ]

# query_tuple_greaterthan = [('greaterthan', 'WHENMODIFIED', lookback_date)]
# # response = connection.ar_payments.get_by_query(fields=fields, and_filter=query_tuple_greaterthan)
# response = connection.ar_payments.get_by_query(fields=fields)
# df = pd.DataFrame(response)

# con.to_sql(df, 'tblARPayments', schema='stage', if_exists='replace', index=False)

# con.run("EXEC eggy.stpARPayments")


#%%