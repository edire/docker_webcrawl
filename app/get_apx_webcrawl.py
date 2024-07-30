#%%

import _webcrawl_tools as wc
from prefect import task, flow


import dlogging
logger = dlogging.NewLogger(__file__, use_cd=True)


@task
def RunCrawler(file_list):
    df_crawl = wc.CrawlPath(file_list)
    driver, temp_dir = wc.GetWebDriver()
    wc.WebCrawlProcess(driver, df_crawl, temp_dir)
    driver.quit()


#%%

@flow
def crawl_all():

    # Sales Report DR
    t01 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_sales_dr.json'])

    # Renewal Contracts
    t02 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_renewal_contracts.json'])

    # RO Market
    t03 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_ro_market.json'])

    # Marketplace Accruals
    t04 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_marketplace_accruals.json'], wait_for=[t01])

    # Revenue Analytics
    t05 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_revenue_analytics.json'], wait_for=[t02])

    # Invoices
    t06 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_invoices.json'], wait_for=[t03])

    # Campaign Pipeline
    t07 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/report_campaign_pipeline.json'], wait_for=[t04])

    # Expirations
    t08 = RunCrawler.submit(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/report_expirations.json'], wait_for=[t05])


crawl_all()

# Run Additional Procs
df_crawl = wc.CrawlPath(['./webcrawl/report_run_procs.json'])
wc.WebCrawlProcess(driver=None, df_scrape=df_crawl)


logger.info('Done, No Problems!')


#%% Testing

# driver = RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_invoices.json'])


# file_list = ['./webcrawl/test.json']
# df_crawl = wc.CrawlPath(file_list)
# wc.WebCrawlProcess(driver, df_crawl)


#%%