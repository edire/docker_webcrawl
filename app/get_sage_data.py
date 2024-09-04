#%%

import os
import _get_sage_data as sd
from prefect import task, flow
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)


#%%

@task
def get_company1():
    logger.info('Sage - Getting Company 1')
    company1 = sd.GetSageConnection(os.getenv('company_id'))
    sd.ARInvoices(company1, suffix='')
    sd.CustomerData(company1, suffix='')
    sd.PaymentDetails(company1)
    logger.info('Sage - Finished Company 1')

@task
def get_company2():
    logger.info('Sage - Getting Company 2')
    company2 = sd.GetSageConnection(os.getenv('company_id2'))
    sd.ARInvoices(company2, suffix='_E150')
    sd.CustomerData(company2, suffix='_E150')
    logger.info('Sage - Finished Company 2')

@task
def get_company3():
    logger.info('Sage - Getting Company 3')
    company3 = sd.GetSageConnection(os.getenv('company_id3'))
    sd.ARInvoices(company3, suffix="_E160")
    sd.CustomerData(company3, suffix="_E160")
    logger.info('Sage - Finished Company 3')


#%%

@flow
def pipeline():
    t_company1 = get_company1.submit()
    t_company2 = get_company2.submit(wait_for=[t_company1])
    get_company3.submit(wait_for=[t_company2])


pipeline()


#%%