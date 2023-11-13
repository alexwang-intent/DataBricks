# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# COMMAND ----------

def get_last_friday_date_str(today):
    today_weekday_idx = today.weekday()
    friday_weekday_idx = 5
    if 0 <= today_weekday_idx < 5:
        today_weekday_idx += 8

    days_since_friday = today_weekday_idx - friday_weekday_idx
    date_last_friday = today - relativedelta(days=days_since_friday)
    date_last_friday_str = date_last_friday.strftime("%Y%m%d")
    # print("last friday's date: ", date_last_friday)
    # print("last friday's date as str: ", date_last_friday_str)
    return date_last_friday_str

def year_month_hive_filter_utility(year_start, year_end, month_start, month_end):
    year_list_str = ''
    year_list = sorted(list(set([f"'{year_start}'", f"'{year_end}'"])))
    for fstring in year_list:
        year_list_str += fstring
        if fstring != year_list[-1]:
            year_list_str += ', '

    month_list_str = ''
    month_list = sorted(list(set([f"'{month_start}'", f"'{month_end}'"])))
    for fstring in month_list:
        month_list_str += fstring
        if fstring != month_list[-1]:
            month_list_str += ', '
    return year_list_str, month_list_str    

# COMMAND ----------

# MAGIC %md
# MAGIC # weekly refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## version weekly_0_0

# COMMAND ----------

##### variable setting
intent_pattern_name = 'TSIP' #"tsip_no_noise"
intent_pattern_tag = 'TSIP' #"tsip_no_noise"#''# #'TSIP', TSIP_123'#
agg_period = 7
# version = 'weekly_0_0'select
##### automated variables
datetime_now = datetime.now()
datetime_now_str = datetime_now.strftime("%Y-%m-%d")
print(datetime_now, datetime_now_str)
yyyymmdd_str =  get_last_friday_date_str(today=datetime_now.date())# for prod date should always be a Friday if agg_period = 7, '20240101', '20230106'
# yyyymmdd_str = '20231013' # for prod date should always be a Friday if agg_period = 7

# target_score_table = 'ihq_prd_usertbls.intent_pattern_weekly_line_scores'
target_score_table = 'ihq_prd_usertbls.ip_weekly_lines_scores_test'
target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights'
# target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights_test'

# yyyymmdd_str = '20230721' # date should always be a Friday
yyyymmdd_end = datetime.strptime(yyyymmdd_str, '%Y%m%d')
yyyymmdd_end_str = datetime.strftime(yyyymmdd_end, "%Y%m%d")
yyyymmdd_start = yyyymmdd_end  - relativedelta(days=agg_period - 1)
yyyymmdd_start_str = datetime.strftime(yyyymmdd_start, "%Y%m%d")
print("ihq_prd_allvm.cust_inet_brwsng_new_v timestamp filters: ", yyyymmdd_start, yyyymmdd_end+relativedelta(days=1))
print(yyyymmdd_str, yyyymmdd_end_str, yyyymmdd_start_str)

year_end = yyyymmdd_end.year
month_end = yyyymmdd_end.month
day_end = yyyymmdd_end.day
year_start = yyyymmdd_start.year
month_start = yyyymmdd_start.month
day_start = yyyymmdd_start.day
# temp_churn_date_str = temp_churn_date.strftime("%Y-%m-%d")
# print("running for churn date: ", temp_churn_date_str)
score_period = f"{year_start:04d}"+f"{month_start:02d}"+f"{day_start:02d}"+"_thru_"+f"{year_end:04d}"+f"{month_end:02d}"+f"{day_end:02d}"

# ('dynatrace.att.com','www.att.com','att-app.quantummetric.com','att.mpeasylink.com','gvpcertvideos.att.com','att-sync.quantummetric.com')
# ('app.yournavi.com','www.yournavi.com','images.yournavi.com','ext.yournavi.com','yournavi.com')
# ('casi.t-mobile.com','smetrics.t-mobile.com','www.t-mobile.com','brass.account.t-mobile.com','tmobile-mkt-prod1-lb.campaign.adobe.com','cdn.tmobile.com')
# (bank = 1 AND account = 2) OR (bank = 3 AND account = 4) OR
print(intent_pattern_name)
print(score_period)
print(year_start, " to ", year_end, " set: ", *{year_start, year_end})
print(month_start, " to ", month_end, " set: ", *{month_start, month_end})
print(target_weight_table)
print(target_score_table)

year_list_str, month_list_str = year_month_hive_filter_utility(year_start, year_end, month_start, month_end)
print("year filter: ", year_list_str, "\nmonth filter: ", month_list_str)

# COMMAND ----------

print(f"------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == 'tmo_switch') and year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE http_host like '%speedtest.t-mobile.com%') b WHERE year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_tag}' as intent_pattern_tag, e.week, 'weekly_0_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## weekly_1_6_0_2 (weekly_1_6 previously)

# COMMAND ----------

##### variable setting
intent_pattern_tag = 'tsip_logistic_1_6' #"tsip_no_noise"#''# #'TSIP', TSIP_123'#
# version = 'weekly_1_6_0_2' hard coded
agg_period = 7

##### automated variables
datetime_now = datetime.now()
datetime_now_str = datetime_now.strftime("%Y-%m-%d")
print(datetime_now, datetime_now_str)
yyyymmdd_str =  get_last_friday_date_str(today=datetime_now.date())
# target_score_table = 'ihq_prd_usertbls.intent_pattern_weekly_line_scores'
target_score_table = 'ihq_prd_usertbls.ip_weekly_lines_scores_test'
# target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights'
target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights_test'

# yyyymmdd_str = '20230721' # date should always be a Friday
yyyymmdd_end = datetime.strptime(yyyymmdd_str, '%Y%m%d')
yyyymmdd_start = yyyymmdd_end  - relativedelta(days=agg_period - 1)
print("ihq_prd_allvm.cust_inet_brwsng_new_v timestamp filters: ", yyyymmdd_start, yyyymmdd_end+relativedelta(days=1))

year_end, month_end, day_end = yyyymmdd_end.year, yyyymmdd_end.month, yyyymmdd_end.day
year_start, month_start, day_start = yyyymmdd_start.year, yyyymmdd_start.month, yyyymmdd_start.day
score_period = f"{year_start:04d}"+f"{month_start:02d}"+f"{day_start:02d}"+"_thru_"+f"{year_end:04d}"+f"{month_end:02d}"+f"{day_end:02d}"

print(intent_pattern_tag)
print(score_period)
print(month_start, " to ", month_end)
print(target_weight_table)
print(target_score_table)

def year_month_hive_filter_utility(year_start, year_end, month_start, month_end):
    year_list_str = ''
    year_list = sorted(list(set([f"'{year_start}'", f"'{year_end}'"])))
    for fstring in year_list:
        year_list_str += fstring
        if fstring != year_list[-1]:
            year_list_str += ', '

    month_list_str = ''
    month_list = sorted(list(set([f"'{month_start}'", f"'{month_end}'"])))
    for fstring in month_list:
        month_list_str += fstring
        if fstring != month_list[-1]:
            month_list_str += ', '
    return year_list_str, month_list_str

year_list_str, month_list_str = year_month_hive_filter_utility(year_start, year_end, month_start, month_end)
print("year filter: ", year_list_str, "\nmonth filter: ", month_list_str)

# COMMAND ----------

print(f"------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == '{intent_pattern_tag}') and year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\n")

print(f"------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;\n")

print(f"------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;\n")

print(f"------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;\n")

print(f"------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host where intent_pattern_tag == '{intent_pattern_tag}'), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, sum(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id,e.intent_pattern_tag, e.week, 'weekly_1_6_0_2' as version, e.score, '{datetime_now_str}' as qload_dt from e;\n")

print(f"------ subset, scale, and insert into final table, weekly_1_6_0 includes swithc_interest_123 mapping and new thresholding https\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 'tmobile-mkt-prod1-lb.campaign.adobe.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=6 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## weekly_1_6_1_3 (final)

# COMMAND ----------

print(f"with tsip as (select * from ihq_prd_usertbls.ip_weekly_lines_scores_test where intent_pattern_tag == 'TSIP' and week_thru == '{score_period}' and version == 'weekly_0_0'), tsip_logistic as (select * from ihq_prd_usertbls.ip_weekly_lines_scores_test where intent_pattern_tag == 'tsip_logistic_1_6' and week_thru == '{score_period}' and version == 'weekly_1_6_0_2'), z as (select tsip_logistic.line_id, tsip_logistic.intent_pattern_tag, tsip_logistic.week_thru, 'weekly_1_6_1_3' as version, tsip_logistic.score_float, IF(tsip.score is not NULL, (tsip.score+tsip_logistic.score)/2.0, tsip_logistic.score) as temp_score, tsip_logistic.upload_dt from tsip_logistic left join tsip on tsip.line_id=tsip_logistic.line_id) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt,  NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.temp_score asc) as score from z;\n")

# COMMAND ----------

# MAGIC %md
# MAGIC #QA

# COMMAND ----------

print("select intent_pattern_tag, week_thru, version, count(*) from ihq_prd_usertbls.intent_pattern_weekly_line_scores group by intent_pattern_tag, week_thru, version order by intent_pattern_tag, week_thru, version;\nselect intent_pattern_tag, week_thru, version, count(*) from ihq_prd_usertbls.ip_weekly_lines_scores_test group by intent_pattern_tag, week_thru, version order by intent_pattern_tag, week_thru, version;")
