#%%

import os
import pandas as pd
from sageintacctsdk import SageIntacctSDK
import datetime as dt
from ddb.sql import SQL
import dlogging


logger = dlogging.NewLogger(__file__, use_cd=True)
lookback_date = dt.date.today() + dt.timedelta(days=-60)
lookback_date = lookback_date.strftime(r'%m/%d/%Y')

try:

    logger.info('Begin Sage API Data Pull')


    #%%

    logger.info('Establish SQL Connection')
    con = SQL(
        db = os.getenv('sql_db'),
        server = os.getenv('sql_server'),
        uid = os.getenv('sql_uid'),
        pwd = os.getenv('sql_pwd')
        )


    #%%

    logger.info('Create Sage API Connector')
    connection = SageIntacctSDK(
        sender_id = os.getenv('sender_id'),
        sender_password = os.getenv('sender_password'),
        user_id = os.getenv('user_id'),
        company_id = os.getenv('company_id'),
        user_password = os.getenv('user_password')
    )


    #%% AR Invoices

    logger.info('Get ar_invoices data')

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
    response = connection.ar_invoices.get_by_query(fields=fields, and_filter=query_tuple_greaterthan)
    # response = connection.ar_invoices.get_by_query(fields=fields)
    # response = connection.ar_invoices.get_all()
    df = pd.DataFrame(response)

    logger.info('Load ar_invoices to SQL')
    con.to_sql(df, 'tblARInvoices', schema='stage', if_exists='replace', index=False)

    logger.info('Run ar_invoices proc')
    con.run("EXEC eggy.stpARInvoices")


    #%% Customer Data

    logger.info('Get customers data')

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

    response = connection.customers.get_by_query(fields=fields)
    df = pd.DataFrame(response)

    logger.info('Load customers to SQL')
    con.to_sql(df, 'tblCustomers', schema='stage', if_exists='replace', index=False)

    logger.info('Run customers proc')
    con.run("EXEC eggy.stpCustomers")


    #%% Payment Details

    logger.info('Get invoice payment details')

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
    response = connection.ar_payment_detail.get_by_query(fields=fields, and_filter=query_tuple_greaterthan)
    # response = connection.ar_payment_detail.get_by_query(fields=fields)
    df = pd.DataFrame(response)

    logger.info('Load ARPaymentDetails to SQL')
    con.to_sql(df, 'tblARPaymentDetails', schema='stage', if_exists='replace', index=False)

    logger.info('Run ARPaymentDetails proc')
    con.run("EXEC eggy.stpARPaymentDetails")


    #%% Payments

    # logger.info('Get invoice payment details')

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

    # logger.info('Load ARPayments to SQL')
    # con.to_sql(df, 'tblARPayments', schema='stage', if_exists='replace', index=False)

    # logger.info('Run ARPayments proc')
    # con.run("EXEC eggy.stpARPayments")


    #%%

    logger.info(f'Complete - {__name__}')


except Exception as e:
    logger.critical(str(e))
    raise