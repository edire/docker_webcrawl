#%%

import os
import calendar
from datetime import datetime, timedelta
import pandas as pd
from dbharbor.sql import SQL
import dbharbor
from dateutil.relativedelta import relativedelta
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)


#%%

logger.info('Create SQL engine')
engine_vars = {
    'db':os.getenv('sql_db'),
    'server':os.getenv('sql_server'),
    'uid':os.getenv('sql_uid'),
    'pwd':os.getenv('sql_pwd'),
}

engine = SQL(**engine_vars)


#%%

logger.info('Define Functions')

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def days_diff_inclusive(start, end):
    return (end - start).days + 1


def days_diff_months(start_date, intervals):
    end_date = start_date + relativedelta(months=intervals)
    delta = (end_date - start_date).days
    return delta


def project_campaign_revenue(data):
    # Parse the campaign start date.
    campaign_start = parse_date(data['Anticipated Start'])
    total_periods = float(data['Periods'])
    
    # Determine the interval length in days.
    interval_parts = data['Interval'].split(" ")
    if interval_parts[1].lower().startswith("week"):
        interval_days = int(interval_parts[0]) * 7
        total_campaign_days = int(total_periods * interval_days)
    elif interval_parts[1].lower().startswith("day"):
        interval_days = int(interval_parts[0])
        total_campaign_days = int(total_periods * interval_days)
    elif interval_parts[1].lower().startswith("month"):
        intervals = int(interval_parts[0])
        total_campaign_days = days_diff_months(campaign_start, intervals * total_periods)
    else:
        raise ValueError("Unsupported interval format.")
    

    # Compute the campaign's end date (inclusive).
    campaign_end = campaign_start + timedelta(days=total_campaign_days - 1)
    
    # Calculate the daily revenue rate.
    if total_campaign_days == 0:
        daily_rate = 0
    else:
        daily_rate = float(data['CI-AdjustedTotal']) / total_campaign_days

    # Build the monthly projection.
    projection = []
    # Start from the first day of the campaign's start month.
    current_month_start = campaign_start.replace(day=1)

    while current_month_start <= campaign_end:
        # Find the last day of the current month.
        days_in_month = calendar.monthrange(current_month_start.year, current_month_start.month)[1]
        current_month_end = current_month_start.replace(day=days_in_month)
        
        # Determine the overlapping period between the campaign and the current month.
        overlap_start = max(campaign_start, current_month_start)
        overlap_end = min(campaign_end, current_month_end)
        
        overlap_days = days_diff_inclusive(overlap_start, overlap_end) if overlap_end >= overlap_start else 0
        
        # Calculate revenue for the month.
        month_revenue = round(overlap_days * daily_rate)
        month_key = current_month_start.isoformat()[:7]  # e.g., "2025-03"
        month_key = current_month_start.strftime("%m%Y")
        projection.append({'month': month_key, 'revenue': month_revenue})
        
        # Move to the first day of the next month.
        if current_month_start.month == 12:
            next_month_start = current_month_start.replace(year=current_month_start.year + 1, month=1, day=1)
        else:
            next_month_start = current_month_start.replace(month=current_month_start.month + 1, day=1)
        current_month_start = next_month_start

    # Sum up the monthly revenues.
    total_projected_revenue = sum(entry['revenue'] for entry in projection)
    
    return {'projection': projection, 'totalProjectedRevenue': total_projected_revenue}


#%%

logger.info('Pull raw data')
sql = """
select r.account_exec_id as [AE]
    , r.campaign_status as [Status]
    , concat(r.advertiser_name, '|', r.advertiser_id) as [Account]
    , r.campaign_name as [Campaign Name]
    , NULL as [Contract Type]
    , NULL as [Inventory Area]
    , NULL as [Audience]
    , r.anticipated_budget as [Budget]
    , NULL as [Files]
    , NULL as [Due Date/Time]
    , NULL as [Reminder]
    , r.anticipated_interval as [Interval]
    , r.anticipated_rate as [Interval Rate]
    , r.anticipated_periods as [Periods]
    , case when cast(r.anticipated_start_date as date) < cast(getdate() as date) then FORMAT(GETDATE(), 'yyyy-MM-dd') else r.anticipated_start_date end as [Anticipated Start]
    , r.campaign_confidence as [CIPct]
    , r.office as [Office]
    , r.anticipated_total_value as [Total]
    , r.confidence_adjusted_budget as [CI-AdjustedTotal]
    , r.confidence_adjusted_budget as [Projection Total]
    , case when r.anticipated_interval is not null
        and r.anticipated_periods > 0
        and r.confidence_adjusted_budget > 0
        and r.campaign_status = 'Active'
        then 1 else 0 end as run_it
from apx.tblrevenue_projected r
where r.campaign_status in ('Active', 'Deferred')
    and r.anticipated_start_date >= '1900-01-01'
order by r.anticipated_start_date
"""

df_details = engine.read(sql)


#%%

logger.info('Get Projections')
df = pd.DataFrame()
for i in range(df_details.shape[0]):

    run_it = df_details.iloc[i]['run_it']

    if run_it == 1:
        df_campaign = df_details.iloc[i].copy()
        campaign = df_campaign.to_dict()
        result = project_campaign_revenue(campaign)

        df_revenue = pd.DataFrame(result['projection']).set_index('month')
        df_revenue.columns = [i]
        df_campaign = pd.DataFrame(df_campaign)
        df_campaign.columns = [i]
        df_temp = pd.concat([df_campaign, df_revenue], axis=0).T
        df = pd.concat([df, df_temp], axis=0)
    else:
        df_temp = df_details.iloc[i]
        df_temp = pd.DataFrame(df_temp).T
        df = pd.concat([df, df_temp], axis=0)



keep_clmns = list(df_details.columns)
keep_clmns.remove('run_it')
current_date = datetime.now()
months = []
for i in range(12):
    new_date = current_date + relativedelta(months=i)
    formatted_date = new_date.strftime("%m%Y")
    months.append(formatted_date)
keep_clmns.extend(months)


#%%

logger.info('Send to SQL')
df_final = df[keep_clmns].copy()
df_final = dbharbor.clean(df_final, rowloadtime=True, drop_cols=False)
engine.to_sql(df_final, 'tblCampaigns_calc', schema='stage', if_exists='replace', index=False)


#%%

# engine.run('EXEC eggy.stpCampaigns')
# engine.run('EXEC eggy.stpCampaigns_Trans')