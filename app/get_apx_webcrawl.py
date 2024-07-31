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
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_sales_dr.json'])

# Renewal Contracts
logger.info('Beginning Renewal Contracts')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_renewal_contracts.json'])

# RO Market
logger.info('Beginning Ro Market')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_ro_market.json'])

# Marketplace Accruals
logger.info('Beginning Marketplace Accruals')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_marketplace_accruals.json'])

# Revenue Analytics
logger.info('Beginning Revenue Analytics')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_revenue_analytics.json'])

# Invoices
logger.info('Beginning Invoices')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_reports_tab.json', './webcrawl/report_invoices.json'])

# Campaign Pipeline
logger.info('Beginning Campaign Pipeline')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/report_campaign_pipeline.json'])

# Expirations
logger.info('Beginning Expirations')
RunCrawler(['./webcrawl/_login.json', './webcrawl/_accounts_tab.json', './webcrawl/report_expirations.json'])

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