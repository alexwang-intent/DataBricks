# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

##### variable setting
intent_pattern_name = 'TSIP' #"tsip_no_noise"
intent_pattern_tag = 'TSIP' #"tsip_no_noise"#''# #'TSIP', TSIP_123'#
agg_period = 7
# version = 'weekly_0_0'select
yyyymmdd_str = '20231027' # for prod date should always be a Friday if agg_period = 7, '20240101', '20230106'
# yyyymmdd_str = '20231013' # for prod date should always be a Friday if agg_period = 7
target_score_table = 'ihq_prd_usertbls.intent_pattern_weekly_line_scores'
# target_score_table = 'ihq_prd_usertbls.ip_weekly_lines_scores_test'
target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights'
# target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights_test'

##### automated variables
datetime_now = datetime.now()
datetime_now_str = datetime_now.strftime("%Y-%m-%d")
print(datetime_now, datetime_now_str)

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

outbounds_temp = spark.sql("select * from vz_feeds.outbound_offers_new where campaign_id in ('CRM_M_CU_MMG_PU_RG_23_05_MOTHERSDAY','CRM_M_CU_AC_EM_RG_23_04_EARTH_DAY', 'CRM_M_CU_MMG_EM_RG_23_04_EARTHDAY','CRM_M_CU_MMG_PU_REW_23_05_JONASBROSPRESALE', 'CRM_M_CU_MMG_PU_REW_23_05_JANELLEMONAEPRESALE', 'CRM_M_CU_MMG_PU_REW_23_04_HIPHOP','CRM_M_CU_AC_EM_RG_23_03_OESINGLET2', 'CRM_M_CU_AC_EM_RG_23_03_OESINGLET2LC');")
outbounds_temp.cache()
print(outbounds_temp.count())

# COMMAND ----------

feed_df = spark.read.format('delta').load("s3://226109243659-vzw-data-science-backups/lake/interim_feed_store/outbound_offers_new/202305/")
display(feed_df)

# COMMAND ----------

display(feed_df2.select("campaign_date").distinct())

# COMMAND ----------

display(feed_df.agg(F.countDistinct("_col1")))

# COMMAND ----------

display(feed_df2.agg(F.countDistinct("line_id")))
display(feed_df.select("_col0", "_col1").join(feed_df2.select("account_id", "line_id"), how='inner', on=[feed_df._col1==feed_df2.line_id]).agg(F.countDistinct("_col1")))

# COMMAND ----------

feed_df2 = spark.sql("select * from vz_feeds.outbound_offers_new where report_month =='2023-04-01'")
display(feed_df2)

# COMMAND ----------



# COMMAND ----------

display(outbounds_temp.filter(F.col("campaign_date")>'2023-06-01').count())

# COMMAND ----------

display(outbounds_temp.select("campaign_date").distinct())

# COMMAND ----------

display(outbounds_temp.groupby("campaign_id", "customer_response").agg(F.count('line_id').alias('count_responses')))

# COMMAND ----------

display(outbounds_temp.groupby("campaign_id", "customer_response").agg(F.countDistinct('line_id').alias('count_responses')))

# COMMAND ----------

# datetime_now_str = '2023-05-16'

# COMMAND ----------

gsip_123 = spark.read.parquet("s3://226109243659-vzw-data-export/prod/odbc_weekly_line_scores/").toDF("line_id", "intent_pattern_tag", "week_thru", "version", "score_float", "upload_dt", "score")
display(gsip_123)
display(gsip_123.groupby("version").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # weekly job script

# COMMAND ----------

# MAGIC %md
# MAGIC # Weight Table Initializations

# COMMAND ----------

# MAGIC %md
# MAGIC ## TSIP POC scoring

# COMMAND ----------

# weekly
data=[
    ('TSIP_123', 'casi.t-mobile.com', 1, 1),
    ('TSIP_123', 'smetrics.t-mobile.com', 0.9002128792, 1),
    ('TSIP_123', 'www.t-mobile.com', 0.888681923, 1),
    ('TSIP_123', 'ocsp.pki.goog', 0.8771509668, 3),
    ('TSIP_123', 'dpm.demdex.net', 0.5387617527, 3),
    ('TSIP_123', 'graph.facebook.com', 0.5285612915, 3),
    ('TSIP_123', 'www.google.com', 0.4886464431, 3),
    ('TSIP_123', 'assets.adobedtm.com', 0.3746673763, 3),
    ('TSIP_123', 'r3.o.lencr.org', 0.2839719709, 3),
    ('TSIP_123', 'incoming.telemetry.mozilla.org', 0.278428242, 3),
    ('TSIP_123', 'speedtest.t-mobile.com', 0.2593578144, 1),
    ('TSIP_123', 'brass.account.t-mobile.com', 0.2562533262, 1),
    ('TSIP_123', 'firebaselogging-pa.googleapis.com', 0.2260954408, 3),
    ('TSIP_123', 'app-measurement.com', 0.2103512507, 3),
    ('TSIP_123', 'pnapi.invoca.net', 0.1950505588, 3),
    ('TSIP_123', 'bag.itunes.apple.com', 0.1861805925, 3),
    ('TSIP_123', 'ocsp.r2m01.amazontrust.com', 0.167110165, 3),
    ('TSIP_123', 'ocsp.r2m02.amazontrust.com', 0.1489267341, 3),
    ('TSIP_123', 'mcias-va7.cloud.adobe.io', 0.1382827745, 3),
    ('TSIP_123', 'www.googletagmanager.com', 0.1373957779, 3),
    ('TSIP_123', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.1240908285, 1),
    ('TSIP_123', 'cdn.tmobile.com', 0.1156643605, 1),
    ('TSIP_123', 'tmobile-app.quantummetric.com', 0.1096771332, 1),
    ('TSIP_123', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.1076813908, 1),
    ('TSIP_123', 'ocsp.digicert.com', 0.1066613447, 3),
    ('TSIP_123', 'bat.bing.com', 0.09969842115, 3),
    ('TSIP_123', 'ocsp2.apple.com', 0.09304594643, 3),
    ('TSIP_123', 'tmobile-sync.quantummetric.com', 0.0921589498, 1),
    ('TSIP_123', 'ocsp.rootca1.amazontrust.com', 0.08572822423, 3),
    ('TSIP_123', 'tmobile.demdex.net', 0.08506297676, 1),
    ('TSIP_123', 'xp.apple.com', 0.08373248182, 3),
    ('TSIP_123', 'www.google-analytics.com', 0.08306723434, 3),
    ('TSIP_123', 'cdn.quantummetric.com', 0.08062799361, 3),
    ('TSIP_123', 'googleads.g.doubleclick.net', 0.07641475962, 3),
    ('TSIP_123', 'geolocation.onetrust.com', 0.07641475962, 3),
    ('TSIP_123', 'www.facebook.com', 0.07486251552, 3),
    ('TSIP_123', 'adservice.google.com', 0.07197977648, 3),
    ('TSIP_123', 'analytics.google.com', 0.06909703743, 3),
    ('TSIP_123', 'rl.quantummetric.com', 0.06909703743, 3),
    ('TSIP_123', 'tmobile.tt.omtrdc.net', 0.06377505765, 1),
    ('TSIP_123', 'tmobilees.mpeasylink.com', 0.05113535569, 1),
    ('TSIP_123', 't-mobile.scene7.com', 0.04625687422, 1),
    ('TSIP_123', 'sgtm.t-mobile.com', 0.03982614866, 1),
    ('TSIP_123', 'mov.t-mobile.com', 0.03539116551, 1),
    ('TSIP_123', 'secure.message.t-mobile.com', 0.02873869079, 1),
    ('TSIP_123', 'cdn.styleguide.t-mobile.com', 0.0229732127, 1),
    ('TSIP_123', 'unav.t-mobile.com', 0.01853822955, 1),
    ('TSIP_123', 'fast.tmobile.demdex.net', 0.01765123293, 1),
    ('TSIP_123', 'zn3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.01587723967, 3),
    ('TSIP_123', 'tmobile.sc.omtrdc.net', 0.01543374135, 1),
    ('TSIP_123', 'appd-geo.geo.t-mobile.com', 0.01144225652, 1),
    ('TSIP_123', 'zn_3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.004568032641, 3),
    ('TSIP_123', 'www.yournavi.com', 0.001685293596, 2),
    ('TSIP_123', 'tools.t-mobile.com', 0.001685293596, 1),
    ('TSIP_123', 'www.mintmobile.com', 0.001685293596, 2),
    ('TSIP_123', 'www.consumercellular.com', 0.001020046124, 2),
    ('TSIP_123', 'sli.tomsguide.com', 0.001020046124, 2),
    ('TSIP_123', 'www.metrobyt-mobile.com', 0.0007982969665, 1),
    ('TSIP_123', 'smetrics.metrobyt-mobile.com', 0.0007982969665, 1),
    ('TSIP_123', 'contentkit.t-mobile.com', 0.0007982969665, 1),
    ('TSIP_123', 'www.whistleout.com', 0.0005765478091, 2),
    ('TSIP_123', 'r3.whistleout.com', 0.0005765478091, 2),
    ('TSIP_123', 'www.tomsguide.com', 0.0005765478091, 2),
    ('TSIP_123', 'core.saas.api.t-mobile.com', 0.0003547986518, 1),
    ('TSIP_123', 'zn7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0003547986518, 3),
    ('TSIP_123', 'account.t-mobile.com', 0.0001330494944, 1),
    ('TSIP_123', 'tmobile.15gifts.com', 0.0001330494944, 1),
    ('TSIP_123', 'martech.metrobyt-mobile.com', 0.0001330494944, 1),
    ('TSIP_123', 'splk-hec.t-mobile.com', 0.0001330494944, 1),
    ('TSIP_123', 'chat.metrobyt-mobile.com', 0.0001330494944, 1),
    ('TSIP_123', 'zn_7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0001330494944, 3),
    ('TSIP_123', 'www.boostmobile.com', 0.0001330494944, 2),
    ('TSIP_123', 'www.redpocket.com', 0.0001330494944, 2),
    ('TSIP_123', 't-mobile.com', 0.0001330494944, 1),
    ('TSIP_123', 'cdn.mintmobile.com', 0.0001330494944, 2),
    ('TSIP_123', 'mint-mobile.58dp.net', 0.0001330494944, 2),
    ('TSIP_123', 'vf.mintmobile.com', 0.0001330494944, 2),
    ('TSIP_123', 'assets.mintmobile.com', 0.0001330494944, 2),
    ('TSIP_123', 't-mobile.7eer.net', 0.0001330494944, 1),
    ('TSIP_123', 'www.t-mobilesavings.com', 0.0001330494944, 1),
    ('TSIP_123', 'tomsguide.com', 0.0001330494944, 2),
    ('TSIP_123', 'hawk.tomsguide.com', 0.0001330494944, 2),
    ('TSIP_123', 'www.yournavi.com', 0.0, 2),
    ('TSIP_123', 'images.yournavi.com', 0.0, 2),
    ('TSIP_123', 'ext.yournavi.com', 0.0, 2),
    ('TSIP_123', 'yournavi.com', 0.0, 2),
    ('TSIP_123', 'sli.tomsguide.com', 0.0, 2),
    ('TSIP_123', 'www.whistleout.com', 0.0, 2),
    ('TSIP_123', 'r3.whistleout.com', 0.0, 2),
    ('TSIP_123', 'www.tomsguide.com', 0.0, 2),
    ('TSIP_123', 'tomsguide.com', 0.0, 2),
    ('TSIP_123', 'www.reviews.org', 0.0, 2),
    ('TSIP_123', 'd.mail.reviews.org', 0.0, 2),
    ('TSIP_123', 'mobile.reviews.org', 0.0, 2),
    ('TSIP_123', 'go.reviews.org', 0.0, 2),
    ('TSIP_123', 'r3.whistleout.com', 0.0, 2),
    ('TSIP_123', 'www.usmobile.com', 0.0, 2),
    ('TSIP_123', 'api.usmobile.com', 0.0, 2),
    ('TSIP_123', 'www.usmobile.com', 0.0, 2),
    ('TSIP_123', 'widget.reviews.co.uk', 0.0, 2),
    ('TSIP_123', 'api.reviews.co.uk', 0.0, 2),
    ('TSIP_123', 'www.bestphoneplans.net', 0.0, 2)]
columns=["intent_pattern_tag", "http_host", "weight", "type"]
display(spark.createDataFrame(data, columns))

# COMMAND ----------

print(f"-----v.1.0prod table create statement\nDROP TABLE ihq_prd_usertbls.intent_pattern_weights_test;CREATE TABLE ihq_prd_usertbls.intent_pattern_weights_test (intent_pattern_tag string, http_host string, weight float, behavior_type tinyint);")
"---v.1.0\n
INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weights_test VALUES ('TSIP_123', 'casi.t-mobile.com', 1, 1),
;"

# COMMAND ----------



# COMMAND ----------

# print(f"-----v.0.2prod table create statement\nDROP TABLE ihq_prd_usertbls.intent_pattern_weights;CREATE TABLE ihq_prd_usertbls.intent_pattern_weights (intent_pattern_tag string, http_host string, weight float, upload_dt string);")
print(f"---v.0.2\nINSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('tmo_switch', 'ocsp.pki.goog', 1.0, '2023-04-28'), ('tmo_switch', 'casi.t-mobile.com', 0.7214997832683138, '2023-04-28'), ('tmo_switch', 'speedtest.t-mobile.com', 0.6337234503684438, '2023-04-28'), ('tmo_switch', 'www.t-mobile.com', 0.5654529692241006, '2023-04-28'), ('tmo_switch', 'smetrics.t-mobile.com', 0.5199393151278717, '2023-04-28'), ('tmo_switch', 'dpm.demdex.net', 0.3638925010836584, '2023-04-28'), ('tmo_switch', 'graph.facebook.com', 0.34655396618985695, '2023-04-28'), ('tmo_switch', 'assets.adobedtm.com', 0.23493714781100997, '2023-04-28'), ('tmo_switch', 'ocsp.rootca1.amazontrust.com', 0.20947117468573906, '2023-04-28'), ('tmo_switch', 'brass.account.t-mobile.com', 0.15583008235804074, '2023-04-28'), ('tmo_switch', 'bag.itunes.apple.com', 0.15149544863459039, '2023-04-28'), ('tmo_switch', 'r3.o.lencr.org', 0.12982228001733856, '2023-04-28'), ('tmo_switch', 'firebaselogging-pa.googleapis.com', 0.12440398786302558, '2023-04-28'), ('tmo_switch', 'ocsp2.apple.com', 0.11681837884698744, '2023-04-28'), ('tmo_switch', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.0951452102297356, '2023-04-28'), ('tmo_switch', 'mcias-va7.cloud.adobe.io', 0.09081057650628523, '2023-04-28'), ('tmo_switch', 'www.google.com', 0.09081057650628523, '2023-04-28'), ('tmo_switch', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.07563935847420893, '2023-04-28'), ('tmo_switch', 'adservice.google.com', 0.06046814044213264, '2023-04-28'), ('tmo_switch', 'www.google-analytics.com', 0.056133506718682266, '2023-04-28'), ('tmo_switch', 'ocsp.digicert.com', 0.053641092327698287, '2023-04-28'), ('tmo_switch', 'stats.g.doubleclick.net', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'tmobile.demdex.net', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'www.facebook.com', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'googleads.g.doubleclick.net', 0.04746423927178153, '2023-04-28'), ('tmo_switch', 'www.googletagmanager.com', 0.04312960554833117, '2023-04-28'), ('tmo_switch', 'connect.facebook.net', 0.03879497182488079, '2023-04-28'), ('tmo_switch', 'clients1.google.com', 0.03391850888599913, '2023-04-28'), ('tmo_switch', 'gateway.icloud.com', 0.025791070654529687, '2023-04-28'), ('tmo_switch', 'api-glb-ause2a.smoot.apple.com', 0.010782401387082788, '2023-04-28'), ('tmo_switch', 'p103-quota.icloud.com', 0.005851755526657996, '2023-04-28'), ('tmo_switch', 'mesu.apple.com', 0.004984828781967923, '2023-04-28'), ('tmo_switch', 'www.mintmobile.com', 0.00411790203727785, '2023-04-28'), ('tmo_switch', 'sli.tomsguide.com', 0.002492414390983962, '2023-04-28'), ('tmo_switch', 'www.whistleout.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'r3.whistleout.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'www.tomsguide.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'cdn.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'mint-mobile.58dp.net', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'vf.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'assets.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'tomsguide.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'hawk.tomsguide.com', 0.00032509752925877764, '2023-04-28');")

# COMMAND ----------

print(f"---v.0.2\nINSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('tmo_switch', 'ocsp.pki.goog', 1.0, '2023-04-28'), ('tmo_switch', 'casi.t-mobile.com', 0.7214997832683138, '2023-04-28'), ('tmo_switch', 'speedtest.t-mobile.com', 0.6337234503684438, '2023-04-28'), ('tmo_switch', 'www.t-mobile.com', 0.5654529692241006, '2023-04-28'), ('tmo_switch', 'smetrics.t-mobile.com', 0.5199393151278717, '2023-04-28'), ('tmo_switch', 'dpm.demdex.net', 0.3638925010836584, '2023-04-28'), ('tmo_switch', 'graph.facebook.com', 0.34655396618985695, '2023-04-28'), ('tmo_switch', 'assets.adobedtm.com', 0.23493714781100997, '2023-04-28'), ('tmo_switch', 'ocsp.rootca1.amazontrust.com', 0.20947117468573906, '2023-04-28'), ('tmo_switch', 'brass.account.t-mobile.com', 0.15583008235804074, '2023-04-28'), ('tmo_switch', 'bag.itunes.apple.com', 0.15149544863459039, '2023-04-28'), ('tmo_switch', 'r3.o.lencr.org', 0.12982228001733856, '2023-04-28'), ('tmo_switch', 'firebaselogging-pa.googleapis.com', 0.12440398786302558, '2023-04-28'), ('tmo_switch', 'ocsp2.apple.com', 0.11681837884698744, '2023-04-28'), ('tmo_switch', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.0951452102297356, '2023-04-28'), ('tmo_switch', 'mcias-va7.cloud.adobe.io', 0.09081057650628523, '2023-04-28'), ('tmo_switch', 'www.google.com', 0.09081057650628523, '2023-04-28'), ('tmo_switch', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.07563935847420893, '2023-04-28'), ('tmo_switch', 'adservice.google.com', 0.06046814044213264, '2023-04-28'), ('tmo_switch', 'www.google-analytics.com', 0.056133506718682266, '2023-04-28'), ('tmo_switch', 'ocsp.digicert.com', 0.053641092327698287, '2023-04-28'), ('tmo_switch', 'stats.g.doubleclick.net', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'tmobile.demdex.net', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'www.facebook.com', 0.0517988729952319, '2023-04-28'), ('tmo_switch', 'googleads.g.doubleclick.net', 0.04746423927178153, '2023-04-28'), ('tmo_switch', 'www.googletagmanager.com', 0.04312960554833117, '2023-04-28'), ('tmo_switch', 'connect.facebook.net', 0.03879497182488079, '2023-04-28'), ('tmo_switch', 'clients1.google.com', 0.03391850888599913, '2023-04-28'), ('tmo_switch', 'gateway.icloud.com', 0.025791070654529687, '2023-04-28'), ('tmo_switch', 'api-glb-ause2a.smoot.apple.com', 0.010782401387082788, '2023-04-28'), ('tmo_switch', 'p103-quota.icloud.com', 0.005851755526657996, '2023-04-28'), ('tmo_switch', 'mesu.apple.com', 0.004984828781967923, '2023-04-28'), ('tmo_switch', 'www.mintmobile.com', 0.00411790203727785, '2023-04-28'), ('tmo_switch', 'sli.tomsguide.com', 0.002492414390983962, '2023-04-28'), ('tmo_switch', 'www.whistleout.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'r3.whistleout.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'www.tomsguide.com', 0.0014087559601213698, '2023-04-28'), ('tmo_switch', 'cdn.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'mint-mobile.58dp.net', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'vf.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'assets.mintmobile.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'tomsguide.com', 0.00032509752925877764, '2023-04-28'), ('tmo_switch', 'hawk.tomsguide.com', 0.00032509752925877764, '2023-04-28');")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ATT, Yournavi, and TSIP

# COMMAND ----------

# print(f"DROP TABLE {target_weight_table};CREATE TABLE {target_weight_table} (intent_pattern_tag string, http_host string, weight float, upload_dt string);")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline

# COMMAND ----------

# AT&T SQL Query:
print(f"-----asip_with_noise:\nINSERT INTO TABLE {target_weight_table} VALUES ('asip_with_noise','ocsp.pki.goog',1, NULL, NULL),('asip_with_noise','www.google.com',0.9833525419, NULL, NULL),('asip_with_noise','ocsp.r2m01.amazontrust.com',0.564604943, NULL, NULL),('asip_with_noise','dynatrace.att.com',0.492892816, NULL, NULL),('asip_with_noise','incoming.telemetry.mozilla.org',0.4288641311, NULL, NULL),('asip_with_noise','bag.itunes.apple.com',0.4263029837, NULL, NULL),('asip_with_noise','pnapi.invoca.net',0.3328211039, NULL, NULL),('asip_with_noise','ocsp.r2m02.amazontrust.com',0.3072096299, NULL, NULL),('asip_with_noise','www.att.com',0.3008067614, NULL, NULL),('asip_with_noise','r3.o.lencr.org',0.2995261877, NULL, NULL),('asip_with_noise','app-site-association.cdn-apple.com',0.2713535664, NULL, NULL),('asip_with_noise','www.googletagmanager.com',0.2611089768, NULL, NULL),('asip_with_noise','adservice.google.com',0.255986682, NULL, NULL),('asip_with_noise','googleads.g.doubleclick.net',0.2073248815, NULL, NULL),('asip_with_noise','dpm.demdex.net',0.184274555, NULL, NULL),('asip_with_noise','bat.bing.com',0.1727493917, NULL, NULL),('asip_with_noise','gateway.icloud.com',0.1618645153, NULL, NULL),('asip_with_noise','fonts.gstatic.com',0.1612242285, NULL, NULL),('asip_with_noise','www.googleadservices.com',0.1535407863, NULL, NULL),('asip_with_noise','www.facebook.com',0.1535407863, NULL, NULL),('asip_with_noise','ib.adnxs.com',0.1535407863, NULL, NULL),('asip_with_noise','xp.apple.com',0.1535407863, NULL, NULL),('asip_with_noise','www.gstatic.com',0.1381739019, NULL, NULL),('asip_with_noise','calendar.google.com',0.1381739019, NULL, NULL),('asip_with_noise','connect.facebook.net',0.1228070175, NULL, NULL),('asip_with_noise','ocsp.digicert.com',0.1212703291, NULL, NULL),('asip_with_noise','configuration.ls.apple.com',0.1151235754, NULL, NULL),('asip_with_noise','cdn.quantummetric.com',0.1074401332, NULL, NULL),('asip_with_noise','s.amazon-adsystem.com',0.1023178384, NULL, NULL),('asip_with_noise','fonts.googleapis.com',0.0984761173, NULL, NULL),('asip_with_noise','ocsp.sectigo.com',0.0971955436, NULL, NULL),('asip_with_noise','servedby.flashtalking.com',0.09207324882, NULL, NULL),('asip_with_noise','mr.fullstory.com',0.09207324882, NULL, NULL),('asip_with_noise','cdn.ampproject.org',0.08951210142, NULL, NULL),('asip_with_noise','ad.doubleclick.net',0.08438980663, NULL, NULL),('asip_with_noise','www.paygonline.com',0.08310923294, NULL, NULL),('asip_with_noise','privacy-policy.truste.com',0.08182865924, NULL, NULL),('asip_with_noise','tpc.googlesyndication.com',0.07670636445, NULL, NULL),('asip_with_noise','brain.foresee.com',0.07670636445, NULL, NULL),('asip_with_noise','att-app.quantummetric.com',0.057497759, NULL, NULL),('asip_with_noise','att.mpeasylink.com',0.0562171853, NULL, NULL),('asip_with_noise','gvpcertvideos.att.com',0.0536560379, NULL, NULL),('asip_with_noise','att-sync.quantummetric.com',0.05109489051, NULL, NULL),('asip_with_noise','signin.att.com',0.05109489051, NULL, NULL),('asip_with_noise','tchosted.firstnet.att.com',0.03572800615, NULL, NULL),('asip_with_noise','oidc.idp.clogin.att.com',0.03572800615, NULL, NULL),('asip_with_noise','services.att.com',0.03572800615, NULL, NULL),('asip_with_noise','att.inq.com',0.03060571136, NULL, NULL),('asip_with_noise','smetrics.att.com',0.02548341657, NULL, NULL),('asip_with_noise','signin-static-js.att.com',0.0126776796, NULL, NULL),('asip_with_noise','attservicesinc.tt.omtrdc.net',0.0113971059, NULL, NULL),('asip_with_noise','tchosted.att.com',0.0113971059, NULL, NULL),('asip_with_noise','att-wireless.official-coupons.com',0.01011653221, NULL, NULL),('asip_with_noise','m.att.com',0.01011653221, NULL, NULL),('asip_with_noise','geolink-igw.cloud.att.com',0.007555384812, NULL, NULL),('asip_with_noise','www.consumercellular.com',0.006274811115, NULL, NULL),('asip_with_noise','sli.tomsguide.com',0.006274811115, NULL, NULL),('asip_with_noise','chclm.att.com',0.006274811115, NULL, NULL),('asip_with_noise','www.metrobyt-mobile.com',0.004994237418, NULL, NULL),('asip_with_noise','cobrowse-att.inq.com',0.004994237418, NULL, NULL),('asip_with_noise','www.cricketwireless.com',0.004994237418, NULL, NULL),('asip_with_noise','www.whistleout.com',0.003713663721, NULL, NULL),('asip_with_noise','r3.whistleout.com',0.003713663721, NULL, NULL),('asip_with_noise','www.tomsguide.com',0.003713663721, NULL, NULL),('asip_with_noise','www.yournavi.com',0.003713663721, NULL, NULL),('asip_with_noise','0.ecom.attccc.com',0.003713663721, NULL, NULL),('asip_with_noise','att-internet.official-coupons.com',0.001152516327, NULL, NULL),('asip_with_noise','www.redpocket.com',0.001152516327, NULL, NULL),('asip_with_noise','tomsguide.com',0.001152516327, NULL, NULL),('asip_with_noise','hawk.tomsguide.com',0.001152516327, NULL, NULL),('asip_with_noise','sentitlement2.mobile.att.net',0.001152516327, NULL, NULL),('asip_with_noise','signin-static-mjs.att.com',0.001152516327, NULL, NULL),('asip_with_noise','cloauth.idp.clogin.att.com',0.001152516327, NULL, NULL),('asip_with_noise','att.com',0.001152516327, NULL, NULL);")

print(f"-----yournavi_with_noise:\nINSERT INTO TABLE {target_weight_table} VALUES ('yournavi_with_noise', 'ocsp.pki.goog', 1.0, NULL, NULL),('yournavi_with_noise', 'ocsp.r2m01.amazontrust.com', 0.8437278022446371, NULL, NULL),('yournavi_with_noise', 'app.yournavi.com', 0.8252592697826395, NULL, NULL),('yournavi_with_noise', 'www.google.com', 0.5240801250177582, NULL, NULL),('yournavi_with_noise', 'ocsp.r2m02.amazontrust.com', 0.44310271345361557, NULL, NULL),('yournavi_with_noise', 'ocsp.rootca1.amazontrust.com', 0.3834351470379315, NULL, NULL),('yournavi_with_noise', 'www.facebook.com', 0.30671970450348063, NULL, NULL),('yournavi_with_noise', 'www.google-analytics.com', 0.2385282000284131, NULL, NULL),('yournavi_with_noise', 'stats.g.doubleclick.net', 0.2385282000284131, NULL, NULL),('yournavi_with_noise', 'fonts.gstatic.com', 0.23000426196902965, NULL, NULL),('yournavi_with_noise', 'www.googletagmanager.com', 0.21721835487995456, NULL, NULL),('yournavi_with_noise', 'ocsp.rootca3.amazontrust.com', 0.2086944168205711, NULL, NULL),('yournavi_with_noise', 'connect.facebook.net', 0.17033669555334566, NULL, NULL),('yournavi_with_noise', 'googleads.g.doubleclick.net', 0.17033669555334566, NULL, NULL),('yournavi_with_noise', 'assets.website-files.com', 0.14902685040488706, NULL, NULL),('yournavi_with_noise', 'r3.o.lencr.org', 0.1419235686887342, NULL, NULL),('yournavi_with_noise', 'www.yournavi.com', 0.13908225600227303, NULL, NULL),('yournavi_with_noise', 'dpm.demdex.net', 0.12771700525642848, NULL, NULL),('yournavi_with_noise', 't.co', 0.12771700525642848, NULL, NULL),('yournavi_with_noise', 'analytics.twitter.com', 0.12771700525642848, NULL, NULL),('yournavi_with_noise', 'in.hotjar.com', 0.12061372354027561, NULL, NULL),('yournavi_with_noise', 'ocsp2.apple.com', 0.11919306719704502, NULL, NULL),('yournavi_with_noise', 'fonts.googleapis.com', 0.11493109816735332, NULL, NULL),('yournavi_with_noise', 'ocsp.sectigo.com', 0.11351044182412275, NULL, NULL),('yournavi_with_noise', 'ocsp.digicert.com', 0.11066912913766157, NULL, NULL),('yournavi_with_noise', 'images.yournavi.com', 0.10924847279443102, NULL, NULL),('yournavi_with_noise', 'api.amplitude.com', 0.10214519107827816, NULL, NULL),('yournavi_with_noise', 'www.googleadservices.com', 0.10214519107827816, NULL, NULL),('yournavi_with_noise', 'analytics.google.com', 0.09930387839181702, NULL, NULL),('yournavi_with_noise', 'unpkg.com', 0.09930387839181702, NULL, NULL),('yournavi_with_noise', 'p37-streams.icloud.com', 0.09788322204858645, NULL, NULL),('yournavi_with_noise', 'static.ads-twitter.com', 0.09362125301889472, NULL, NULL),('yournavi_with_noise', 'pnapi.invoca.net', 0.08935928398920301, NULL, NULL),('yournavi_with_noise', 'gs-loc.apple.com', 0.0850973149595113, NULL, NULL),('yournavi_with_noise', 'adservice.google.com', 0.0850973149595113, NULL, NULL),('yournavi_with_noise', 'us-std-00001.s3.dualstack.us-east-1.amazonaws.com', 0.08083534592981959, NULL, NULL),('yournavi_with_noise', 's.amazon-adsystem.com', 0.07941468958658901, NULL, NULL),('yournavi_with_noise', 's.adroll.com', 0.07941468958658901, NULL, NULL),('yournavi_with_noise', 'static.hotjar.com', 0.079414689586589, NULL, NULL),('yournavi_with_noise', 'ext.yournavi.com', 0.069470095183975, NULL, NULL),('yournavi_with_noise', 'yournavi.com', 0.025429748543827246, NULL, NULL),('yournavi_with_noise', 'sli.tomsguide.com', 0.006961216081829805, NULL, NULL),('yournavi_with_noise', 'www.whistleout.com', 0.004119903395368661, NULL, NULL),('yournavi_with_noise', 'r3.whistleout.com', 0.004119903395368661, NULL, NULL),('yournavi_with_noise', 'www.tomsguide.com', 0.004119903395368661, NULL, NULL),('yournavi_with_noise', 'tomsguide.com', 0.0012785907089075154, NULL, NULL),('yournavi_with_noise', 'hawk.tomsguide.com', 0.0012785907089075154, NULL, NULL);")

print(f"-----tsip:\nINSERT INTO TABLE {target_weight_table} VALUES ('tsip_with_noise', 'casi.t-mobile.com', 1, NULL, NULL),('tsip_with_noise', 'smetrics.t-mobile.com', 0.9002128792, NULL, NULL),('tsip_with_noise', 'www.t-mobile.com', 0.888681923, NULL, NULL),('tsip_with_noise', 'ocsp.pki.goog', 0.8771509668, NULL, NULL),('tsip_with_noise', 'dpm.demdex.net', 0.5387617527, NULL, NULL),('tsip_with_noise', 'graph.facebook.com', 0.5285612915, NULL, NULL),('tsip_with_noise', 'www.google.com', 0.4886464431, NULL, NULL),('tsip_with_noise', 'assets.adobedtm.com', 0.3746673763, NULL, NULL),('tsip_with_noise', 'r3.o.lencr.org', 0.2839719709, NULL, NULL),('tsip_with_noise', 'incoming.telemetry.mozilla.org', 0.278428242, NULL, NULL),('tsip_with_noise', 'speedtest.t-mobile.com', 0.2593578144, NULL, NULL),('tsip_with_noise', 'brass.account.t-mobile.com', 0.2562533262, NULL, NULL),('tsip_with_noise', 'firebaselogging-pa.googleapis.com', 0.2260954408, NULL, NULL),('tsip_with_noise', 'app-measurement.com', 0.2103512507, NULL, NULL),('tsip_with_noise', 'pnapi.invoca.net', 0.1950505588, NULL, NULL),('tsip_with_noise', 'bag.itunes.apple.com', 0.1861805925, NULL, NULL),('tsip_with_noise', 'ocsp.r2m01.amazontrust.com', 0.167110165, NULL, NULL),('tsip_with_noise', 'ocsp.r2m02.amazontrust.com', 0.1489267341, NULL, NULL),('tsip_with_noise', 'mcias-va7.cloud.adobe.io', 0.1382827745, NULL, NULL),('tsip_with_noise', 'www.googletagmanager.com', 0.1373957779, NULL, NULL),('tsip_with_noise', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.1240908285, NULL, NULL),('tsip_with_noise', 'cdn.tmobile.com', 0.1156643605, NULL, NULL),('tsip_with_noise', 'tmobile-app.quantummetric.com', 0.1096771332, NULL, NULL),('tsip_with_noise', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.1076813908, NULL, NULL),('tsip_with_noise', 'ocsp.digicert.com', 0.1066613447, NULL, NULL),('tsip_with_noise', 'bat.bing.com', 0.09969842115, NULL, NULL),('tsip_with_noise', 'ocsp2.apple.com', 0.09304594643, NULL, NULL),('tsip_with_noise', 'tmobile-sync.quantummetric.com', 0.0921589498, NULL, NULL),('tsip_with_noise', 'ocsp.rootca1.amazontrust.com', 0.08572822423, NULL, NULL),('tsip_with_noise', 'tmobile.demdex.net', 0.08506297676, NULL, NULL),('tsip_with_noise', 'xp.apple.com', 0.08373248182, NULL, NULL),('tsip_with_noise', 'www.google-analytics.com', 0.08306723434, NULL, NULL),('tsip_with_noise', 'cdn.quantummetric.com', 0.08062799361, NULL, NULL),('tsip_with_noise', 'googleads.g.doubleclick.net', 0.07641475962, NULL, NULL),('tsip_with_noise', 'geolocation.onetrust.com', 0.07641475962, NULL, NULL),('tsip_with_noise', 'www.facebook.com', 0.07486251552, NULL, NULL),('tsip_with_noise', 'adservice.google.com', 0.07197977648, NULL, NULL),('tsip_with_noise', 'rl.quantummetric.com', 0.06909703743, NULL, NULL),('tsip_with_noise', 'analytics.google.com', 0.06909703743, NULL, NULL),('tsip_with_noise', 'tmobile.tt.omtrdc.net', 0.06377505765, NULL, NULL),('tsip_with_noise', 'tmobilees.mpeasylink.com', 0.05113535569, NULL, NULL),('tsip_with_noise', 't-mobile.scene7.com', 0.04625687422, NULL, NULL),('tsip_with_noise', 'sgtm.t-mobile.com', 0.03982614866, NULL, NULL),('tsip_with_noise', 'mov.t-mobile.com', 0.03539116551, NULL, NULL),('tsip_with_noise', 'secure.message.t-mobile.com', 0.02873869079, NULL, NULL),('tsip_with_noise', 'cdn.styleguide.t-mobile.com', 0.0229732127, NULL, NULL),('tsip_with_noise', 'unav.t-mobile.com', 0.01853822955, NULL, NULL),('tsip_with_noise', 'fast.tmobile.demdex.net', 0.01765123293, NULL, NULL),('tsip_with_noise', 'zn3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.01587723967, NULL, NULL),('tsip_with_noise', 'tmobile.sc.omtrdc.net', 0.01543374135, NULL, NULL),('tsip_with_noise', 'appd-geo.geo.t-mobile.com', 0.01144225652, NULL, NULL),('tsip_with_noise', 'zn_3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.004568032641, NULL, NULL),('tsip_with_noise', 'www.yournavi.com', 0.001685293596, NULL, NULL),('tsip_with_noise', 'tools.t-mobile.com', 0.001685293596, NULL, NULL),('tsip_with_noise', 'www.mintmobile.com', 0.001685293596, NULL, NULL),('tsip_with_noise', 'www.consumercellular.com', 0.001020046124, NULL, NULL),('tsip_with_noise', 'sli.tomsguide.com', 0.001020046124, NULL, NULL),('tsip_with_noise', 'www.metrobyt-mobile.com', 0.0007982969665, NULL, NULL),('tsip_with_noise', 'smetrics.metrobyt-mobile.com', 0.0007982969665, NULL, NULL),('tsip_with_noise', 'contentkit.t-mobile.com', 0.0007982969665, NULL, NULL),('tsip_with_noise', 'www.whistleout.com', 0.0005765478091, NULL, NULL),('tsip_with_noise', 'r3.whistleout.com', 0.0005765478091, NULL, NULL),('tsip_with_noise', 'www.tomsguide.com', 0.0005765478091, NULL, NULL),('tsip_with_noise', 'core.saas.api.t-mobile.com', 0.0003547986518, NULL, NULL),('tsip_with_noise', 'zn7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0003547986518, NULL, NULL),('tsip_with_noise', 'account.t-mobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'tmobile.15gifts.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'martech.metrobyt-mobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'splk-hec.t-mobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'chat.metrobyt-mobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'zn_7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'www.boostmobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'www.redpocket.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 't-mobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'cdn.mintmobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'mint-mobile.58dp.net', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'vf.mintmobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'assets.mintmobile.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 't-mobile.7eer.net', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'www.t-mobilesavings.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'tomsguide.com', 0.0001330494944, NULL, NULL),('tsip_with_noise', 'hawk.tomsguide.com', 0.0001330494944, NULL, NULL);")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline + Noise Removal

# COMMAND ----------

# print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_weights_test;CREATE TABLE ihq_prd_usertbls.intent_pattern_weights")

# COMMAND ----------

print(f"-----asip_no_noise:\nINSERT INTO TABLE {target_weight_table} VALUES ('asip_no_noise', 'dynatrace.att.com', 0.492892816, NULL, NULL), ('asip_no_noise', 'incoming.telemetry.mozilla.org', 0.4288641311, NULL, NULL), ('asip_no_noise', 'pnapi.invoca.net', 0.3328211039, NULL, NULL), ('asip_no_noise', 'www.att.com', 0.3008067614, NULL, NULL), ('asip_no_noise', 'r3.o.lencr.org', 0.2995261877, NULL, NULL), ('asip_no_noise', 'dpm.demdex.net', 0.184274555, NULL, NULL), ('asip_no_noise', 'bat.bing.com', 0.1727493917, NULL, NULL), ('asip_no_noise', 'fonts.gstatic.com', 0.1612242285, NULL, NULL), ('asip_no_noise', 'ib.adnxs.com', 0.1535407863, NULL, NULL), ('asip_no_noise', 'www.gstatic.com', 0.1381739019, NULL, NULL), ('asip_no_noise', 'ocsp.digicert.com', 0.1212703291, NULL, NULL), ('asip_no_noise', 'cdn.quantummetric.com', 0.1074401332, NULL, NULL), ('asip_no_noise', 'ocsp.sectigo.com', 0.0971955436, NULL, NULL), ('asip_no_noise', 'servedby.flashtalking.com', 0.09207324882, NULL, NULL), ('asip_no_noise', 'mr.fullstory.com', 0.09207324882, NULL, NULL), ('asip_no_noise', 'cdn.ampproject.org', 0.08951210142, NULL, NULL), ('asip_no_noise', 'ad.doubleclick.net', 0.08438980663, NULL, NULL), ('asip_no_noise', 'www.paygonline.com', 0.08310923294, NULL, NULL), ('asip_no_noise', 'privacy-policy.truste.com', 0.08182865924, NULL, NULL), ('asip_no_noise', 'brain.foresee.com', 0.07670636445, NULL, NULL), ('asip_no_noise', 'att-app.quantummetric.com', 0.057497759, NULL, NULL), ('asip_no_noise', 'att.mpeasylink.com', 0.0562171853, NULL, NULL), ('asip_no_noise', 'gvpcertvideos.att.com', 0.0536560379, NULL, NULL), ('asip_no_noise', 'att-sync.quantummetric.com', 0.05109489051, NULL, NULL), ('asip_no_noise', 'signin.att.com', 0.05109489051, NULL, NULL), ('asip_no_noise', 'tchosted.firstnet.att.com', 0.03572800615, NULL, NULL), ('asip_no_noise', 'oidc.idp.clogin.att.com', 0.03572800615, NULL, NULL), ('asip_no_noise', 'services.att.com', 0.03572800615, NULL, NULL), ('asip_no_noise', 'att.inq.com', 0.03060571136, NULL, NULL), ('asip_no_noise', 'smetrics.att.com', 0.02548341657, NULL, NULL), ('asip_no_noise', 'signin-static-js.att.com', 0.0126776796, NULL, NULL), ('asip_no_noise', 'attservicesinc.tt.omtrdc.net', 0.0113971059, NULL, NULL), ('asip_no_noise', 'tchosted.att.com', 0.0113971059, NULL, NULL), ('asip_no_noise', 'att-wireless.official-coupons.com', 0.01011653221, NULL, NULL), ('asip_no_noise', 'm.att.com', 0.01011653221, NULL, NULL), ('asip_no_noise', 'geolink-igw.cloud.att.com', 0.007555384812, NULL, NULL), ('asip_no_noise', 'www.consumercellular.com', 0.006274811115, NULL, NULL), ('asip_no_noise', 'sli.tomsguide.com', 0.006274811115, NULL, NULL), ('asip_no_noise', 'chclm.att.com', 0.006274811115, NULL, NULL), ('asip_no_noise', 'www.metrobyt-mobile.com', 0.004994237418, NULL, NULL), ('asip_no_noise', 'cobrowse-att.inq.com', 0.004994237418, NULL, NULL), ('asip_no_noise', 'www.cricketwireless.com', 0.004994237418, NULL, NULL), ('asip_no_noise', 'www.whistleout.com', 0.003713663721, NULL, NULL), ('asip_no_noise', 'r3.whistleout.com', 0.003713663721, NULL, NULL), ('asip_no_noise', 'www.tomsguide.com', 0.003713663721, NULL, NULL), ('asip_no_noise', 'www.yournavi.com', 0.003713663721, NULL, NULL), ('asip_no_noise', '0.ecom.attccc.com', 0.003713663721, NULL, NULL), ('asip_no_noise', 'att-internet.official-coupons.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'www.redpocket.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'tomsguide.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'hawk.tomsguide.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'sentitlement2.mobile.att.net', 0.001152516327, NULL, NULL), ('asip_no_noise', 'signin-static-mjs.att.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'cloauth.idp.clogin.att.com', 0.001152516327, NULL, NULL), ('asip_no_noise', 'att.com', 0.001152516327, NULL, NULL);")

print(f"-----yournavi_no_noise:\nINSERT INTO TABLE {target_weight_table} VALUES ('yournavi_no_noise', 'app.yournavi.com', 0.8252592698, NULL, NULL), ('yournavi_no_noise', 'stats.g.doubleclick.net', 0.2385282, NULL, NULL), ('yournavi_no_noise', 'fonts.gstatic.com', 0.230004262, NULL, NULL), ('yournavi_no_noise', 'assets.website-files.com', 0.1490268504, NULL, NULL), ('yournavi_no_noise', 'r3.o.lencr.org', 0.1419235687, NULL, NULL), ('yournavi_no_noise', 'www.yournavi.com', 0.139082256, NULL, NULL), ('yournavi_no_noise', 't.co', 0.1277170053, NULL, NULL), ('yournavi_no_noise', 'analytics.twitter.com', 0.1277170053, NULL, NULL), ('yournavi_no_noise', 'dpm.demdex.net', 0.1277170053, NULL, NULL), ('yournavi_no_noise', 'in.hotjar.com', 0.1206137235, NULL, NULL), ('yournavi_no_noise', 'ocsp.sectigo.com', 0.1135104418, NULL, NULL), ('yournavi_no_noise', 'ocsp.digicert.com', 0.1106691291, NULL, NULL), ('yournavi_no_noise', 'images.yournavi.com', 0.1092484728, NULL, NULL), ('yournavi_no_noise', 'api.amplitude.com', 0.1021451911, NULL, NULL), ('yournavi_no_noise', 'unpkg.com', 0.09930387839, NULL, NULL), ('yournavi_no_noise', 'static.ads-twitter.com', 0.09362125302, NULL, NULL), ('yournavi_no_noise', 'pnapi.invoca.net', 0.08935928399, NULL, NULL), ('yournavi_no_noise', 's.adroll.com', 0.07941468959, NULL, NULL), ('yournavi_no_noise', 'static.hotjar.com', 0.07941468959, NULL, NULL), ('yournavi_no_noise', 'ext.yournavi.com', 0.06947009518, NULL, NULL), ('yournavi_no_noise', 'yournavi.com', 0.02542974854, NULL, NULL), ('yournavi_no_noise', 'sli.tomsguide.com', 0.006961216082, NULL, NULL), ('yournavi_no_noise', 'www.whistleout.com', 0.004119903395, NULL, NULL), ('yournavi_no_noise', 'r3.whistleout.com', 0.004119903395, NULL, NULL), ('yournavi_no_noise', 'www.tomsguide.com', 0.004119903395, NULL, NULL), ('yournavi_no_noise', 'tomsguide.com', 0.001278590709, NULL, NULL), ('yournavi_no_noise', 'hawk.tomsguide.com', 0.001278590709, NULL, NULL);")

print(f"-----tsip_no_noise:\nINSERT INTO TABLE {target_weight_table} VALUES ('tsip_no_noise', 'casi.t-mobile.com', 1, NULL, NULL), ('tsip_no_noise', 'smetrics.t-mobile.com', 0.9002128792, NULL, NULL), ('tsip_no_noise', 'www.t-mobile.com', 0.888681923, NULL, NULL), ('tsip_no_noise', 'dpm.demdex.net', 0.5387617527, NULL, NULL), ('tsip_no_noise', 'assets.adobedtm.com', 0.3746673763, NULL, NULL), ('tsip_no_noise', 'r3.o.lencr.org', 0.2839719709, NULL, NULL), ('tsip_no_noise', 'incoming.telemetry.mozilla.org', 0.278428242, NULL, NULL), ('tsip_no_noise', 'speedtest.t-mobile.com', 0.2593578144, NULL, NULL), ('tsip_no_noise', 'brass.account.t-mobile.com', 0.2562533262, NULL, NULL), ('tsip_no_noise', 'app-measurement.com', 0.2103512507, NULL, NULL), ('tsip_no_noise', 'pnapi.invoca.net', 0.1950505588, NULL, NULL), ('tsip_no_noise', 'mcias-va7.cloud.adobe.io', 0.1382827745, NULL, NULL), ('tsip_no_noise', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.1240908285, NULL, NULL), ('tsip_no_noise', 'cdn.tmobile.com', 0.1156643605, NULL, NULL), ('tsip_no_noise', 'tmobile-app.quantummetric.com', 0.1096771332, NULL, NULL), ('tsip_no_noise', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.1076813908, NULL, NULL), ('tsip_no_noise', 'ocsp.digicert.com', 0.1066613447, NULL, NULL), ('tsip_no_noise', 'bat.bing.com', 0.09969842115, NULL, NULL), ('tsip_no_noise', 'tmobile-sync.quantummetric.com', 0.0921589498, NULL, NULL), ('tsip_no_noise', 'tmobile.demdex.net', 0.08506297676, NULL, NULL), ('tsip_no_noise', 'cdn.quantummetric.com', 0.08062799361, NULL, NULL), ('tsip_no_noise', 'geolocation.onetrust.com', 0.07641475962, NULL, NULL), ('tsip_no_noise', 'rl.quantummetric.com', 0.06909703743, NULL, NULL), ('tsip_no_noise', 'tmobile.tt.omtrdc.net', 0.06377505765, NULL, NULL), ('tsip_no_noise', 'tmobilees.mpeasylink.com', 0.05113535569, NULL, NULL), ('tsip_no_noise', 't-mobile.scene7.com', 0.04625687422, NULL, NULL), ('tsip_no_noise', 'sgtm.t-mobile.com', 0.03982614866, NULL, NULL), ('tsip_no_noise', 'mov.t-mobile.com', 0.03539116551, NULL, NULL), ('tsip_no_noise', 'secure.message.t-mobile.com', 0.02873869079, NULL, NULL), ('tsip_no_noise', 'cdn.styleguide.t-mobile.com', 0.0229732127, NULL, NULL), ('tsip_no_noise', 'unav.t-mobile.com', 0.01853822955, NULL, NULL), ('tsip_no_noise', 'fast.tmobile.demdex.net', 0.01765123293, NULL, NULL), ('tsip_no_noise', 'zn3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.01587723967, NULL, NULL), ('tsip_no_noise', 'tmobile.sc.omtrdc.net', 0.01543374135, NULL, NULL), ('tsip_no_noise', 'appd-geo.geo.t-mobile.com', 0.01144225652, NULL, NULL), ('tsip_no_noise', 'zn_3edajdvgwonv9nr-tmobilecx.siteintercept.qualtrics.com', 0.004568032641, NULL, NULL), ('tsip_no_noise', 'www.yournavi.com', 0.001685293596, NULL, NULL), ('tsip_no_noise', 'tools.t-mobile.com', 0.001685293596, NULL, NULL), ('tsip_no_noise', 'www.mintmobile.com', 0.001685293596, NULL, NULL), ('tsip_no_noise', 'www.consumercellular.com', 0.001020046124, NULL, NULL), ('tsip_no_noise', 'sli.tomsguide.com', 0.001020046124, NULL, NULL), ('tsip_no_noise', 'www.metrobyt-mobile.com', 0.0007982969665, NULL, NULL), ('tsip_no_noise', 'smetrics.metrobyt-mobile.com', 0.0007982969665, NULL, NULL), ('tsip_no_noise', 'contentkit.t-mobile.com', 0.0007982969665, NULL, NULL), ('tsip_no_noise', 'www.whistleout.com', 0.0005765478091, NULL, NULL), ('tsip_no_noise', 'r3.whistleout.com', 0.0005765478091, NULL, NULL), ('tsip_no_noise', 'www.tomsguide.com', 0.0005765478091, NULL, NULL), ('tsip_no_noise', 'core.saas.api.t-mobile.com', 0.0003547986518, NULL, NULL), ('tsip_no_noise', 'zn7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0003547986518, NULL, NULL), ('tsip_no_noise', 'account.t-mobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'tmobile.15gifts.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'martech.metrobyt-mobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'splk-hec.t-mobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'chat.metrobyt-mobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'zn_7nj5omfyyzlxou5-tmobilecx.siteintercept.qualtrics.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'www.boostmobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'www.redpocket.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 't-mobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'cdn.mintmobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'mint-mobile.58dp.net', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'vf.mintmobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'assets.mintmobile.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 't-mobile.7eer.net', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'www.t-mobilesavings.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'tomsguide.com', 0.0001330494944, NULL, NULL), ('tsip_no_noise', 'hawk.tomsguide.com', 0.0001330494944, NULL, NULL);")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logistic Scoring (noise weighted)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## weekly refresh (TEST no thresholds)

# COMMAND ----------

"""
yournavi_with_noise,
yournavi_no_noise,
yournavi_logistic,
asip_with_noise,
asip_no_noise,
asip_logistic,
tsip_with_noise,
tsip_no_noise,
tmo_switch_logistic"""
intent_pattern_tag = 'tsip_no_noise'

# COMMAND ----------

print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' and month <= '{(yyyymmdd_end).month}' AND http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag = '{intent_pattern_tag}') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host where b.intent_pattern_tag == '{intent_pattern_tag}'), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, sum(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_tag}' as intent_pattern_tag, e.week, 'weekly_0_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
# print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")
print(f"\n------ (NO SUBSET OR SCALING) insert into final table\nwith z as (select line_id, intent_pattern_tag, week as week_thru, version, score as score_float, qload_dt as upload_dt from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## qa

# COMMAND ----------

# the following queries may be explored to clean out non-signal contributing hosts, aka visited by most lines
# part 0.b1
# need to clean out the 
print(f"select max(distinct_line_id_count) from ihq_prd_usertbls.web_marginal_counts where year >= '{year_start}' and month >= '{month_start}' and day >= '{day_start}' limit 10;")
# part 0.b2
# need to clean out the 
print(f"select stemmed_domain, avg(distinct_line_id_count) from ihq_prd_usertbls.web_marginal_counts where stemmed_domain in (select distinct(http_host) as stemmed_domain from ihq_prd_usertbls.intent_pattern_weights where intent_pattern_tag = 'tmo_switch') and year >= '{year_start}' and month >= '{month_start}' and day >= '{day_start}'  group by stemmed_domain;")

# COMMAND ----------

# MAGIC %md
# MAGIC # weekly refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## version weekly_0_0

# COMMAND ----------

# print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag = 'tmo_switch') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == 'tmo_switch') and year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE http_host like '%speedtest.t-mobile.com%') b WHERE year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_tag}' as intent_pattern_tag, e.week, 'weekly_0_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
# print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score) INSERT INTO TABLE {target_score_table} select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score, g.qload_dt as upload_dt from g cross join f;\n")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------

with churners as ( select * from ihq_historical_customer_churn_v where line_term_dt > '2023-10-15'), churners_active as (select  ihq_historical_customer_churn_v.line_id, ihq_historical_customer_churn_v.qload_dt, churners.line_term_dt from )


# COMMAND ----------

# MAGIC %md
# MAGIC ## X version weekly_1_0

# COMMAND ----------

print(f"------- method for automatically thresholding audience size by idealized high scoring lines\nWITH scaled_weight AS (SELECT distinct intent_pattern_tag ,http_host, weight*100 as scaled_weight from {target_weight_table} where behavior_type == 1 and intent_pattern_tag == '{intent_pattern_tag}'), top_green_hosts AS (SELECT http_host, scaled_weight, RANK() OVER (PARTITION BY intent_pattern_tag ORDER BY scaled_weight DESC) AS http_rank from scaled_weight) select * from top_green_hosts order by http_rank asc;")

# COMMAND ----------

# this version was intended to implement a business rule to filter use only lines with some yellow dot activity as part of the final target pool. query was not updated properly and final audience ended up being a similar audience as weekly_0_0 but with different weightings
print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND month <= '{month_end}' AND http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == '{intent_pattern_tag}') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------lines with at least 1 yellow dot\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered_lines;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered_lines\n(line_id   string);\nwith twos as (select distinct(http_host) as http_host from {target_weight_table} where behavior_type == 2 and intent_pattern_tag == '{intent_pattern_tag}')\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly_filtered_lines select distinct(c.line_id) as line_id from ihq_prd_usertbls.intent_pattern_weekly c inner join twos on c.http_host = twos.http_host;")
print(f"\n-------weblogs filtered on yellow dot lines\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered\n(line_id string,\nyear string,\nmonth string,\nday string,\nhour string,\nhttp_host string);\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly_filtered select z.line_id, z.year, z.month, z.day, z.hour, z.http_host FROM ihq_prd_usertbls.intent_pattern_weekly z where z.line_id in (select line_id from ihq_prd_usertbls.intent_pattern_weekly_filtered_lines);")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_name}' as intent_pattern_tag, e.week, 'weekly_1_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## version weekly_1_1

# COMMAND ----------

print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND month <= '{month_end}' AND http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == '{intent_pattern_tag}') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------lines with at least 1 yellow dot\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered_lines;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered_lines\n(line_id   string);\nwith twos as (select distinct(http_host) as http_host from {target_weight_table} where behavior_type == 2 and intent_pattern_tag == '{intent_pattern_tag}')\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly_filtered_lines select distinct(c.line_id) as line_id from ihq_prd_usertbls.intent_pattern_weekly c inner join twos on c.http_host = twos.http_host;")
print(f"\n-------weblogs filtered on yellow dot lines\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered\n(line_id string,\nyear string,\nmonth string,\nday string,\nhour string,\nhttp_host string);\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly_filtered select z.line_id, z.year, z.month, z.day, z.hour, z.http_host FROM ihq_prd_usertbls.intent_pattern_weekly z where z.line_id in (select line_id from ihq_prd_usertbls.intent_pattern_weekly_filtered_lines);")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_name}' as intent_pattern_tag, e.week, 'weekly_1_1' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## weekly_1_4_2
# MAGIC Best of logistic regression methods. Includes a summing instead of averaging weighted host scores.

# COMMAND ----------

"""------ subset weblogs
DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '2023' and month >= '8' AND month <= '8' AND http_host in (select distinct(http_host) as http_host from ihq_prd_usertbls.intent_pattern_weights_test where intent_pattern_tag == 'tmo_switch_logistic') and date_time >= '2023-08-05 00:00:00' AND date_time < '2023-08-12 00:00:00') a;

------ marginal sample (ie. per line_id, lines common denominator) daily and weekly
DROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;
CREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores
(line_id string,
http_host string,
day string,
count_lhd bigint,
count_ld bigint,
countd_ld bigint);
with a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;
INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '20230805_thru_20230811' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;

------ the population (ie. across all line_ids, hosts common denominator) daily and weekly
DROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;
CREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores
(day string,
http_host string,
count_hd bigint,
count_d bigint,
countd_d bigint);
with a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;
INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '20230805_thru_20230811' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;

------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)
DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf
(line_id string,
week string,
http_host string,
tf double,
idf double,
tfidf double);
with tf_table as (select line_id, '20230805_thru_20230811' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '20230805_thru_20230811'), idf_table as (select '20230805_thru_20230811' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '20230805_thru_20230811'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;


------------------------
------ ------ ------ ------ ------ ------ ------ ------ ------ ------
------ subset, scale, and insert into final table
------ ALT VERSION weekly_1_4_2 summation (not average score) and threshold on >=5 out of 8

------ raw intent pattern scores
DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,
intent_pattern_tag string,
week string,
version string,
score double,
qload_dt string);
WITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join ihq_prd_usertbls.intent_pattern_weights_test b on a.http_host=b.http_host where intent_pattern_tag == 'tmo_switch_logistic'), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, sum(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id,e.intent_pattern_tag, e.week, 'weekly_1_4_2' as version, e.score, '2023-09-11' as qload_dt from e;

------ subset, scale, and insert into final table, weekly_1_4_2 includes swithc_interest_123 mapping and new thresholding https
with b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','casi.t-mobile.com','zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com','app-measurement.com','brass.account.t-mobile.com','firebaselogging-pa.googleapis.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '20230805_thru_20230811') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE ihq_prd_usertbls.ip_weekly_lines_scores_test select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## weekly_1_6_0

# COMMAND ----------

##### variable setting
intent_pattern_tag = 'tsip_logistic_1_6' #"tsip_no_noise"#''# #'TSIP', TSIP_123'#
version = 'weekly_1_6_0'
yyyymmdd_str = '20231027' # for prod, date should always be a Friday if agg_period = 7
agg_period = 7
target_score_table = 'ihq_prd_usertbls.intent_pattern_weekly_line_scores'
# target_score_table = 'ihq_prd_usertbls.ip_weekly_lines_scores_test'
# target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights'
target_weight_table = 'ihq_prd_usertbls.intent_pattern_weights_test'

##### automated variables
datetime_now = datetime.now()
datetime_now_str = datetime_now.strftime("%Y-%m-%d")
print(datetime_now, datetime_now_str)

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



# COMMAND ----------

!? TODO convert greater than conditionals on months years days to be "in set" operations instead of "greater/less than" operations. Hive applies ordering to strings based on left most string character so for example month string "12" is less than month string "5"

# COMMAND ----------

print(f"------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_filtered AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag == '{intent_pattern_tag}') and year in ({year_list_str}) and month in ({month_list_str}) and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\n")

print(f"------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly_filtered group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;\n")

print(f"------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly_filtered group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;\n")

print(f"------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;\n")

print(f"------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host where intent_pattern_tag == '{intent_pattern_tag}'), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, sum(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id,e.intent_pattern_tag, e.week, '{version}' as version, e.score, '{datetime_now_str}' as qload_dt from e;\n")

print(f"------ subset, scale, and insert into final table, weekly_1_6_0 includes swithc_interest_123 mapping and new thresholding https\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','casi.t-mobile.com','zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com','app-measurement.com','brass.account.t-mobile.com','firebaselogging-pa.googleapis.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #QA

# COMMAND ----------

select intent_pattern_tag, week_thru, version, count(*) from ihq_prd_usertbls.intent_pattern_weekly_line_scores group by intent_pattern_tag, week_thru, version order by intent_pattern_tag, week_thru, version;
select week_thru, version, count(*) from ihq_prd_usertbls.ip_weekly_lines_scores_test group by week_thru, version;

# COMMAND ----------

!? TODO convert greater than conditionals on months years days to be "in set" operations instead of "greater/less than" operations. Hive applies ordering to strings based on left most string character so for example month string "12" is less than month string "5"

# COMMAND ----------

print(f"DROP TABLE ihq_prd_usertbls.ip_weekly_lines_scores_test;\nCREATE TABLE ihq_prd_usertbls.ip_weekly_lines_scores_test\n(line_id string,\nintent_pattern_tag string,\nweek_thru string,\nversion string, \nscore_float double,\nupload_dt string,\nscore tinyint);")

# COMMAND ----------

# print("ALTER TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores CHANGE score score_float DOUBLE;")
# print("ALTER TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores ADD COLUMNS (score TINYINT);")

# COMMAND ----------

print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host in (select distinct(http_host) as http_host from {target_weight_table} where intent_pattern_tag = '{intent_pattern_tag}') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join {target_weight_table} b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intent_pattern_name}' as intent_pattern_tag, e.week, '{version}' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
# print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score) INSERT INTO TABLE {target_score_table} select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score, g.qload_dt as upload_dt from g cross join f;\n")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, z.score_float, z.upload_dt, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score from z;\n")

# COMMAND ----------

print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score), z as (select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score_float, g.qload_dt as upload_dt from g cross join f) INSERT INTO TABLE {target_score_table} select z.line_id, z.intent_pattern_tag, z.week_thru, z.version, NTILE(100) OVER(PARTITION BY z.week_thru ORDER BY z.score_float asc) as score, z.upload_dt, z.score_float from z;\n")

# COMMAND ----------

select min(score_float) from ihq_prd_usertbls.ip_weekly_lines_scores_test limit 10;
select max(score_float) from ihq_prd_usertbls.ip_weekly_lines_scores_test limit 10;
select min(score) from ihq_prd_usertbls.ip_weekly_lines_scores_test limit 10;
select max(score) from ihq_prd_usertbls.ip_weekly_lines_scores_test limit 10;


# COMMAND ----------

print(f"------lines with 5of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host >=5 and b.day = '{score_period}';")

# COMMAND ----------

print(f"------lines with 1of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==1 and b.day = '{score_period}';")
print(f"------lines with 2of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==2 and b.day = '{score_period}';")
print(f"------lines with 3of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==3 and b.day = '{score_period}';")
print(f"------lines with 4of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==4 and b.day = '{score_period}';")
print(f"------lines with 5of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==5 and b.day = '{score_period}';")
print(f"------lines with 6of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), c as (select * from b inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores d on b.line_id=d.line_id) select count(distinct(c.line_id)) from c where b.count_distinct_host ==6 and b.day = '{score_period}';")


# COMMAND ----------

print(f"------lines with 1of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==1 and b.day = '{score_period}';")
print(f"------lines with 2of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==2 and b.day = '{score_period}';")
print(f"------lines with 3of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==3 and b.day = '{score_period}';")
print(f"------lines with 4of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==4 and b.day = '{score_period}';")
print(f"------lines with 5of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==5 and b.day = '{score_period}';")
print(f"------lines with 6of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==6 and b.day = '{score_period}';")


# COMMAND ----------

print(f"------lines with 6of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_weekly where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host ==6 and b.day = '{score_period}';")
# print(f"SELECT b.line_id, b.year, b.month, b.day, b.hour, "speedtest.t-mobile.com" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '2023' and month >= '7' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '2023-07-15 00:00:00' AND date_time < '2023-07-22 00:00:00';")

# COMMAND ----------

# select * from ihq_prd_usertbls.intent_pattern_weekly where http_host == 'speedtest.t-mobile.com' limit 20; , "speedtest.t-mobile.com" as speed_test
SELECT b.line_id, b.year, b.month, b.day, b.hour, http_host FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '2023' AND month == '7' AND day in ('15', '16', '17', '18', '19', '20', '21') AND http_host like '%speedtest.t-mobile.com%' limit 40;

# COMMAND ----------

# MAGIC %md
# MAGIC # QA

# COMMAND ----------

display(spark.read.parquet("s3://226109243659-vzw-data-export/prod/odbc_weekly_line_scores/000000_0").toDF("line_id", "intent_pattern_tag", "week_thru", "version", "score_float", "upload_dt", "centile").filter(F.col("version")=="weekly_1_0").groupby("week_thru").count())

# COMMAND ----------

display(spark.read.parquet("s3://226109243659-vzw-data-export/prod/odbc_weekly_line_scores/000000_0").toDF("line_id", "intent_pattern_tag", "week_thru", "version", "score_float", "upload_dt", "centile").filter(F.col("version")=="weekly_1_0"))

# COMMAND ----------

# print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c )")

# COMMAND ----------

------ primary lines only
with a as (select distinct(c.line_id) as line_id from ihq_prd_usertbls.intent_pattern_weekly_line_scores c inner join (select d.line_id from ihq_prd_allvm.ihq_mvp_kvp_v d where d.attr_nm_val like "%PRIMARY_LINE=Y%") d on c.line_id=d.line_id), b as (select a.line_id, b.week_thru from a inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores b on a.line_id=b.line_id) select count(distinct(b.line_id)) as count from b;


# COMMAND ----------

print(f"select distinct(week_thru) from ihq_prd_usertbls.intent_pattern_weekly_line_scores;")
print(f"select week_thru, count(*) from ihq_prd_usertbls.intent_pattern_weekly_line_scores group by week_thru order by week_thru asc;")
print("select week_thru, count(*) from ihq_prd_usertbls.intent_pattern_weekly_line_scores where week_thru = '20230729_thru_20230804' group by week_thru order by week_thru asc;")

# COMMAND ----------

# MAGIC %md
# MAGIC # IGNORE...

# COMMAND ----------

##########################
# !? OLD STUFF !?
"""


select line_id, http_host, "22_thru_28" as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.tmo_switch_line_scores where line_id in ("28da866a4a4da3230b3ce450d454ce92d58598cc748abd7e74a7225f8359c2ec", "3ce7e51bbdc4ef4d66a010b0b7416f74552daac2292962a233fad21052768777")group by line_id, http_host;


----------------IHQ odbc host weighting
--- needs to be updated before scoring: ihq_prd_usertbls.odbc_host_weights;\


--------------- final odbc_line_scores
DROP TABLE ihq_prd_usertbls.odbc_weekly_line_scores;
CREATE TABLE ihq_prd_usertbls.odbc_weekly_line_scores (line_id string, odbc_tag string, week string, score double);
WITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.odbc_tag, b.tfdf from tmo_weekly_tfidf a inner join ihq_prd_usertbls.odbc_host_weights b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.odbc_tag, c.week, c.tfidf*c.tfdf as http_host_score from c), e AS (select d.line_id, d.odbc_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.odbc_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.odbc_weekly_line_scores select e.line_id, e.odbc_tag, e.week, e.score from e;
"""

# COMMAND ----------

---------------- export audience to ihq for validation analysis
--- hive
# INSERT OVERWRITE DIRECTORY '/user/svc-omg_ihq_pld/odbc_weekly_line_scores' STORED AS PARQUET SELECT * FROM ihq_prd_usertbls.intent_pattern_weekly_line_scores;
INSERT OVERWRITE DIRECTORY '/user/svc-omg_ihq_pld/odbc_weekly_line_scores' STORED AS PARQUET SELECT * FROM ihq_prd_usertbls.ip_weekly_lines_scores_test;
--- exit

--needs to be to "user" not another dir

"run in linux cli not hive"
cd /data/sL_ihq

rm -r odbc_weekly_line_scores/
mkdir odbc_weekly_line_scores
cd odbc_weekly_line_scores
hadoop fs -copyToLocal /user/svc-omg_ihq_pld/odbc_weekly_line_scores/*
cd ..

# connect to sftp
sftp -i ~/.ssh/id_rsa_omega_odi omega-odi@vpce-055d91a3b8e19aec9-qnhmewxw.server.transfer.us-east-1.vpce.amazonaws.com
# enter password 

sftp> mkdir odbc_weekly_line_scores
sftp> pwd
Remote working directory: /226109243659-vzw-data-export/prod
sftp> lpwd
Local working directory: /data/sL_ihq
sftp> put odbc_weekly_line_scores/* odbc_weekly_line_scores

>bye

# COMMAND ----------

1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(report_month) from vz_feeds.outbound_offers_new;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vz_feeds.outbound_offers_new where report_month == '2023-09-01';

# COMMAND ----------


