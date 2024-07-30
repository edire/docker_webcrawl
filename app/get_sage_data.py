#%%

import os
import _get_sage_data as sd
from prefect import task, flow


#%%

@task(retries=2, retry_delay_seconds=120)
def GetCompany(company_id):
    return sd.GetSageConnection(company_id)

@task(retries=2, retry_delay_seconds=120)
def ARInvoices(sage_con, suffix=''):
    sd.ARInvoices(sage_con, suffix)

@task(retries=2, retry_delay_seconds=120)
def CustomerData(sage_con, suffix=''):
    sd.CustomerData(sage_con, suffix)

@task(retries=2, retry_delay_seconds=120)
def PaymentDetails(sage_con):
    sd.PaymentDetails(sage_con)


#%%

@flow
def run_all():
    company1 = GetCompany.submit(os.getenv('company_id'))
    ARInvoices.submit(company1)
    CustomerData.submit(company1)
    PaymentDetails.submit(company1)
    
    company2 = GetCompany.submit(os.getenv('company_id2'))
    ARInvoices.submit(company2, suffix='_E150')
    CustomerData.submit(company2, suffix='_E150')

    company3 = GetCompany.submit(os.getenv('company_id3'))
    ARInvoices.submit(company3, suffix="_E160")
    CustomerData.submit(company3, suffix="_E160")


#%%

run_all()


#%%