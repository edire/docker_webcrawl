
#%%

import _webcrawl_tools as wc
import dlogging


logger = dlogging.NewLogger(__file__, use_cd=True)


def RunCrawler(file_list):
    df_crawl = wc.CrawlPath(file_list)
    driver, temp_dir = wc.GetWebDriver()
    wc.WebCrawlProcess(driver, df_crawl, temp_dir)
    driver.quit()


#%%

# Sales Report DR
logger.info('Beginning Sales Report DR')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_sales_dr.json'])

# Renewal Contracts
logger.info('Beginning Renewal Contracts')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_renewal_contracts.json'])

# RO Market
logger.info('Beginning Ro Market')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_ro_market.json'])

# Marketplace Accruals
logger.info('Beginning Marketplace Accruals')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_marketplace_accruals.json'])

# Revenue Analytics
logger.info('Beginning Revenue Analytics')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_revenue_analytics.json'])

# Invoices
logger.info('Beginning Invoices')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/reports_invoices.json'])

# Campaign Pipeline
logger.info('Beginning Campaign Pipeline')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/accounts_campaign_pipeline.json'])

# Expirations
logger.info('Beginning Expirations')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/accounts_expirations.json'])

# Contracts
logger.info('Beginning Contracts')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_core_tab.json', './webcrawl/core_contracts.json'])

# Run Additional Procs
df_crawl = wc.CrawlPath(['./webcrawl/report_run_procs.json'])
wc.WebCrawlProcess(driver=None, df_scrape=df_crawl)


logger.info('Done, No Problems!')


#%% Testing

# import os
# os.chdir('./app')
# import _webcrawl_tools as wc


# def RunCrawler(file_list):
#     try:
#         df_crawl = wc.CrawlPath(file_list)
#         driver, temp_dir = wc.GetWebDriver()
#         wc.WebCrawlProcess(driver, df_crawl, temp_dir)
#         return driver, temp_dir
#     except:
#         return driver, temp_dir


# driver, temp_dir = RunCrawler(['./webcrawl/_login.json', './webcrawl/_core_tab.json', './webcrawl/core_contracts.json'])


# file_list = ['./webcrawl/test.json']
# df_crawl = wc.CrawlPath(file_list)
# wc.WebCrawlProcess(driver, df_crawl)


#%%