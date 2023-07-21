# Databricks notebook source
# MAGIC %md
# MAGIC tfidf = 
# MAGIC
# MAGIC lhf = 				how many times a line visited a host on a given day / total hosts visited by line on a given day<BR>
# MAGIC VARIATION: lhf = 	how many times a line visited a host on a given day / total DISTINCT hosts visited by line on a given day<BR>
# MAGIC hf = 				how many times a host was visited on a given day / total hosts visited on a given day<BR>
# MAGIC VARIATION: hf = 	how many times a host was visited on a given day / total DISTINCT GLOBAL hosts visited on a given day<BR>
# MAGIC
# MAGIC -------------- high level org<BR>
# MAGIC for http_hosts:<BR>
# MAGIC 		count						per 	line, 	host, 	day					c_p_lhd<BR>
# MAGIC 		count						per		line, 			day					c_p_ld<BR>
# MAGIC 		count 	DISTINCT 			per		line,			day					cd_p_ld<BR>
# MAGIC 		count		 				per 			host, 	day					c_p_hd<BR>
# MAGIC 		count 				 		per 					day					c_p_d<BR>
# MAGIC 		count 	DISTINCT 	 		per 					day					cd_p_d<BR>
# MAGIC 		tfidf = (c_p_lhd/c_p_ld)*(c_p_d/c_p_hd)<BR>
# MAGIC 		
# MAGIC for http_hosts:<BR>
# MAGIC 		count						per 	line, 	host, 	week<BR>
# MAGIC 		count						per		line, 			week<BR>
# MAGIC 		count 	DISTINCT 			per		line,			week<BR>
# MAGIC 		count		 				per 			host, 	week<BR>
# MAGIC 		count 				 		per 					week<BR>
# MAGIC 		count 	DISTINCT 	 		per 					week<BR>
# MAGIC 		
# MAGIC for http_hosts:<BR>
# MAGIC 		count						per 	line, 	host, 	month
# MAGIC 		count						per		line, 			month
# MAGIC 		count 	DISTINCT 			per		line,			month
# MAGIC 		count		 				per 			host, 	month
# MAGIC 		count 				 		per 					month
# MAGIC 		count 	DISTINCT 	 		per 					month
# MAGIC 		
# MAGIC ---highlight daily signal strength
# MAGIC tfidf = (c_p_lhd/c_p_ld)*(c_p_d/c_p_hd) 
# MAGIC ---highlight weekly signal strength
# MAGIC tfidf = (c_p_lhw/c_p_lw)*(c_p_w/c_p_hw) 
# MAGIC
# MAGIC ----------------- DAILIES
# MAGIC ------the sample (ie. per line_id, lines common denominator)
# MAGIC ---c_p_lhd 
# MAGIC select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.tmo_switch_weekly group by line_id, http_host, day;
# MAGIC ---c_p_ld
# MAGIC select line_id, day, count(*) as count_ld from ihq_prd_usertbls.tmo_switch_weekly group by line_id, day;
# MAGIC ---cd_p_ld
# MAGIC select count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.tmo_switch_weekly group by line_id, day;
# MAGIC ------the population (ie. across all line_ids, hosts common denominator)
# MAGIC ---c_p_hd
# MAGIC select day, http_host, count(*) as count_hd from ihq_prd_usertbls.tmo_switch_weekly group by http_host, day;
# MAGIC ---c_p_d
# MAGIC select day, count(*) as count_d from ihq_prd_usertbls.tmo_switch_weekly group by day;
# MAGIC ---cd_p_d
# MAGIC select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.tmo_switch_weekly group by day;

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime
from dateutil.relativedelta import relativedelta
#needs more spaces!




#

# COMMAND ----------

datetime_now = datetime.now()
datetime_now_str = datetime_now.strftime("%Y-%m-%d")
print(datetime_now, datetime_now_str)

agg_period = 7
yyyymmdd_str = '20230714'
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
print(score_period)
intention_pattern_name = "TSIP"
print(intention_pattern_name)

# COMMAND ----------

# datetime_now_str = '2023-05-16'

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

# MAGIC %md
# MAGIC ### ATT, Yournavi, and TSIP

# COMMAND ----------

# AT&T SQL Query:
"""
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('asip', 'ocsp.pki.goog', 1.0), ('asip', 'dynatrace.att.com', 0.8297046777322697), ('asip', 'pnapi.invoca.net', 0.5602500538909246), ('asip', 'bag.itunes.apple.com', 0.5279154990299633), ('asip', 'www.att.com', 0.5063591291226557), ('asip', 'www.google.com', 0.4826471222246173), ('asip', 'app-site-association.cdn-apple.com', 0.4567794783358482), ('asip', 'ocsp.r2m01.amazontrust.com', 0.3985772795861176), ('asip', 'incoming.telemetry.mozilla.org', 0.2907954300495796), ('asip', 'xp.apple.com', 0.2584608751886182), ('asip', 'adservice.google.com', 0.24121577926277213), ('asip', 'calendar.google.com', 0.23259323129984907), ('asip', 'ocsp.r2m02.amazontrust.com', 0.215348135374003), ('asip', 'googleads.g.doubleclick.net', 0.21103686139254144), ('asip', 'www.googletagmanager.com', 0.20672558741107996), ('asip', 'dpm.demdex.net', 0.20672558741107994), ('asip', 'r3.o.lencr.org', 0.20456995042034917), ('asip', 'gateway.icloud.com', 0.18775598189264922), ('asip', 'mr.fullstory.com', 0.1549902996335417), ('asip', 'servedby.flashtalking.com', 0.1549902996335417), ('asip', 'configuration.ls.apple.com', 0.1549902996335417), ('asip', 'fonts.gstatic.com', 0.1420564776891571), ('asip', 'cdn.quantummetric.com', 0.13990084069842637), ('asip', 'www.paygonline.com', 0.13990084069842637), ('asip', 'privacy-policy.truste.com', 0.1377452037076956), ('asip', 'brain.foresee.com', 0.12912265574477255), ('asip', 'bat.bing.com', 0.12912265574477255), ('asip', 'www.gstatic.com', 0.12050010778184951), ('asip', 'connect.facebook.net', 0.11618883380038798), ('asip', 'cdn.ampproject.org', 0.11618883380038798), ('asip', 'www.facebook.com', 0.11618883380038798), ('asip', 'ocsp.sectigo.com', 0.1097219228281957), ('asip', 'ib.adnxs.com', 0.10756628583746496), ('asip', 'gsp64-ssl.ls.apple.com', 0.10325501185600346), ('asip', 'tpc.googlesyndication.com', 0.09678810088381115), ('asip', 'att-app.quantummetric.com', 0.09678810088381115), ('asip', 'fonts.googleapis.com', 0.09678810088381115), ('asip', 'solutions.invocacdn.com', 0.09678810088381115), ('asip', 'ocsp.digicert.com', 0.09678810088381114), ('asip', 'att.mpeasylink.com', 0.0946324638930804), ('asip', 'gvpcertvideos.att.com', 0.09032118991161887), ('asip', 'signin.att.com', 0.08600991593015735), ('asip', 'att-sync.quantummetric.com', 0.08600991593015735), ('asip', 'tchosted.firstnet.att.com', 0.06014227204138823), ('asip', 'oidc.idp.clogin.att.com', 0.06014227204138823), ('asip', 'services.att.com', 0.06014227204138823), ('asip', 'att.inq.com', 0.05151972407846519), ('asip', 'smetrics.att.com', 0.042897176115542134), ('asip', 'signin-static-js.att.com', 0.02134080620823453), ('asip', 'attservicesinc.tt.omtrdc.net', 0.019185169217503767), ('asip', 'tchosted.att.com', 0.019185169217503767), ('asip', 'm.att.com', 0.01702953222677301), ('asip', 'att-wireless.official-coupons.com', 0.01702953222677301), ('asip', 'geolink-igw.cloud.att.com', 0.01271825824531149), ('asip', 'chclm.att.com', 0.010562621254580727), ('asip', 'sli.tomsguide.com', 0.010562621254580727), ('asip', 'www.cricketwireless.com', 0.008406984263849967), ('asip', 'cobrowse-att.inq.com', 0.008406984263849967), ('asip', 'www.tomsguide.com', 0.006251347273119206), ('asip', 'r3.whistleout.com', 0.006251347273119206), ('asip', 'www.whistleout.com', 0.006251347273119206), ('asip', '0.ecom.attccc.com', 0.006251347273119206), ('asip', 'hawk.tomsguide.com', 0.0019400732916576848), ('asip', 'tomsguide.com', 0.0019400732916576848), ('asip', 'sentitlement2.mobile.att.net', 0.0019400732916576848), ('asip', 'signin-static-mjs.att.com', 0.0019400732916576848), ('asip', 'att-internet.official-coupons.com', 0.0019400732916576848), ('asip', 'cloauth.idp.clogin.att.com', 0.0019400732916576848), ('asip', 'att.com', 0.0019400732916576848);
"""

"""
Yournavi: 
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('yournavi_search', 'ocsp.pki.goog', 1.0), ('yournavi_search', 'ocsp.r2m01.amazontrust.com', 0.8437278022446371), ('yournavi_search', 'app.yournavi.com', 0.8252592697826395), ('yournavi_search', 'www.google.com', 0.5240801250177582), ('yournavi_search', 'ocsp.r2m02.amazontrust.com', 0.44310271345361557), ('yournavi_search', 'ocsp.rootca1.amazontrust.com', 0.3834351470379315), ('yournavi_search', 'www.facebook.com', 0.30671970450348063), ('yournavi_search', 'www.google-analytics.com', 0.2385282000284131), ('yournavi_search', 'stats.g.doubleclick.net', 0.2385282000284131), ('yournavi_search', 'fonts.gstatic.com', 0.23000426196902965), ('yournavi_search', 'www.googletagmanager.com', 0.21721835487995456), ('yournavi_search', 'ocsp.rootca3.amazontrust.com', 0.2086944168205711), ('yournavi_search', 'connect.facebook.net', 0.17033669555334566), ('yournavi_search', 'googleads.g.doubleclick.net', 0.17033669555334566), ('yournavi_search', 'assets.website-files.com', 0.14902685040488706), ('yournavi_search', 'r3.o.lencr.org', 0.1419235686887342), ('yournavi_search', 'www.yournavi.com', 0.13908225600227303), ('yournavi_search', 'dpm.demdex.net', 0.12771700525642848), ('yournavi_search', 't.co', 0.12771700525642848), ('yournavi_search', 'analytics.twitter.com', 0.12771700525642848), ('yournavi_search', 'in.hotjar.com', 0.12061372354027561), ('yournavi_search', 'ocsp2.apple.com', 0.11919306719704502), ('yournavi_search', 'fonts.googleapis.com', 0.11493109816735332), ('yournavi_search', 'ocsp.sectigo.com', 0.11351044182412275), ('yournavi_search', 'ocsp.digicert.com', 0.11066912913766157), ('yournavi_search', 'images.yournavi.com', 0.10924847279443102), ('yournavi_search', 'api.amplitude.com', 0.10214519107827816), ('yournavi_search', 'www.googleadservices.com', 0.10214519107827816), ('yournavi_search', 'analytics.google.com', 0.09930387839181702), ('yournavi_search', 'unpkg.com', 0.09930387839181702), ('yournavi_search', 'p37-streams.icloud.com', 0.09788322204858645), ('yournavi_search', 'static.ads-twitter.com', 0.09362125301889472), ('yournavi_search', 'pnapi.invoca.net', 0.08935928398920301), ('yournavi_search', 'gs-loc.apple.com', 0.0850973149595113), ('yournavi_search', 'adservice.google.com', 0.0850973149595113), ('yournavi_search', 'us-std-00001.s3.dualstack.us-east-1.amazonaws.com', 0.08083534592981959), ('yournavi_search', 's.amazon-adsystem.com', 0.07941468958658901), ('yournavi_search', 's.adroll.com', 0.07941468958658901), ('yournavi_search', 'static.hotjar.com', 0.079414689586589), ('yournavi_search', 'ext.yournavi.com', 0.069470095183975), ('yournavi_search', 'yournavi.com', 0.025429748543827246), ('yournavi_search', 'sli.tomsguide.com', 0.006961216081829805), ('yournavi_search', 'www.whistleout.com', 0.004119903395368661), ('yournavi_search', 'r3.whistleout.com', 0.004119903395368661), ('yournavi_search', 'www.tomsguide.com', 0.004119903395368661), ('yournavi_search', 'tomsguide.com', 0.0012785907089075154), ('yournavi_search', 'hawk.tomsguide.com', 0.0012785907089075154);
"""

"""
TSIP:
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('tsip', 'www.t-mobile.com', 1.0), ('tsip', 'ocsp.pki.goog', 0.9144979426067438), ('tsip', 'smetrics.t-mobile.com', 0.8463634906214929), ('tsip', 'casi.t-mobile.com', 0.8367445091647516), ('tsip', 'dpm.demdex.net', 0.5652754769411638), ('tsip', 'graph.facebook.com', 0.482445358841447), ('tsip', 'www.google.com', 0.43007534868807773), ('tsip', 'assets.adobedtm.com', 0.36862074493667496), ('tsip', 'speedtest.t-mobile.com', 0.3125100197723507), ('tsip', 'incoming.telemetry.mozilla.org', 0.22593918666167898), ('tsip', 'brass.account.t-mobile.com', 0.21311387805269058), ('tsip', 'bag.itunes.apple.com', 0.20563244803078068), ('tsip', 'r3.o.lencr.org', 0.20509806017207283), ('tsip', 'firebaselogging-pa.googleapis.com', 0.1997541815849943), ('tsip', 'pnapi.invoca.net', 0.1970822422914551), ('tsip', 'app-measurement.com', 0.1586063164644899), ('tsip', 'ocsp.r2m01.amazontrust.com', 0.1415059049858387), ('tsip', 'mcias-va7.cloud.adobe.io', 0.1308181478116817), ('tsip', 'tmobile-app.quantummetric.com', 0.1308181478116817), ('tsip', 'www.googletagmanager.com', 0.11612248169721581), ('tsip', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.11585528776786189), ('tsip', 'tmobile.demdex.net', 0.11451931812109227), ('tsip', 'ocsp.r2m02.amazontrust.com', 0.10944263346336769), ('tsip', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.10677069416982843), ('tsip', 'cdn.tmobile.com', 0.1040987548762892), ('tsip', 'ocsp.rootca1.amazontrust.com', 0.10329717308822743), ('tsip', 'xp.apple.com', 0.1008924277240421), ('tsip', 'bat.bing.com', 0.09608293699567143), ('tsip', 'ocsp2.apple.com', 0.0880671191150537), ('tsip', 'tmobile.tt.omtrdc.net', 0.08539517982151444), ('tsip', 'ocsp.digicert.com', 0.08347138353016616), ('tsip', 'geolocation.onetrust.com', 0.08325762838668303), ('tsip', 'app-site-association.cdn-apple.com', 0.08165446481055949), ('tsip', 'www.google-analytics.com', 0.07978410730508202), ('tsip', 'tmobile-sync.quantummetric.com', 0.07737936194089669), ('tsip', 'www.facebook.com', 0.07684497408218884), ('tsip', 'oauthaccountmanager.googleapis.com', 0.07684497408218884), ('tsip', 'cdn.quantummetric.com', 0.07203548335381818), ('tsip', 'adservice.google.com', 0.07043231977769464), ('tsip', 'sgtm.t-mobile.com', 0.05039277507615027), ('tsip', 't-mobile.scene7.com', 0.04798802971196494), ('tsip', 'mov.t-mobile.com', 0.04558328434777961), ('tsip', 'secure.message.t-mobile.com', 0.035964302891038316), ('tsip', 'appd-geo.geo.t-mobile.com', 0.023406188211403833), ('tsip', 'cdn.styleguide.t-mobile.com', 0.022337412493988133), ('tsip', 'unav.t-mobile.com', 0.018596697483033182), ('tsip', 'tools.t-mobile.com', 0.005504194944690856), ('tsip', 'www.mintmobile.com', 0.0020306738630898303), ('tsip', 'sli.tomsguide.com', 0.001229092075028055), ('tsip', 't-mobile.com', 0.00096189814567413), ('tsip', 'www.whistleout.com', 0.0006947042163202051), ('tsip', 'r3.whistleout.com', 0.0006947042163202051), ('tsip', 'www.tomsguide.com', 0.0006947042163202051), ('tsip', 'cdn.mintmobile.com', 0.000160316357612355), ('tsip', 'vf.mintmobile.com', 0.000160316357612355), ('tsip', 'assets.mintmobile.com', 0.000160316357612355), ('tsip', 'contentkit.t-mobile.com', 0.000160316357612355), ('tsip', 't-mobile.7eer.net', 0.000160316357612355), ('tsip', 'www.t-mobilesavings.com', 0.000160316357612355), ('tsip', 'tomsguide.com', 0.000160316357612355), ('tsip', 'hawk.tomsguide.com', 0.000160316357612355), ('tsip', 'mint-mobile.58dp.net', 0.000160316357612355);
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline + Noise Removal

# COMMAND ----------

# AT&T SQL Query:
"""
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('asip', 'dynatrace.att.com', 0.8297046777322697), ('asip', 'pnapi.invoca.net', 0.5602500538909246), ('asip', 'www.att.com', 0.5063591291226557), ('asip', 'incoming.telemetry.mozilla.org', 0.2907954300495796), ('asip', 'dpm.demdex.net', 0.20672558741107994), ('asip', 'r3.o.lencr.org', 0.20456995042034917), ('asip', 'mr.fullstory.com', 0.1549902996335417), ('asip', 'servedby.flashtalking.com', 0.1549902996335417), ('asip', 'fonts.gstatic.com', 0.1420564776891571), ('asip', 'cdn.quantummetric.com', 0.13990084069842637), ('asip', 'www.paygonline.com', 0.13990084069842637), ('asip', 'privacy-policy.truste.com', 0.1377452037076956), ('asip', 'bat.bing.com', 0.12912265574477255), ('asip', 'brain.foresee.com', 0.12912265574477255), ('asip', 'www.gstatic.com', 0.12050010778184951), ('asip', 'cdn.ampproject.org', 0.11618883380038798), ('asip', 'ocsp.sectigo.com', 0.1097219228281957), ('asip', 'ib.adnxs.com', 0.10756628583746496), ('asip', 'solutions.invocacdn.com', 0.09678810088381115), ('asip', 'att-app.quantummetric.com', 0.09678810088381115), ('asip', 'ocsp.digicert.com', 0.09678810088381114), ('asip', 'att.mpeasylink.com', 0.0946324638930804), ('asip', 'gvpcertvideos.att.com', 0.09032118991161887), ('asip', 'signin.att.com', 0.08600991593015735), ('asip', 'att-sync.quantummetric.com', 0.08600991593015735), ('asip', 'tchosted.firstnet.att.com', 0.06014227204138823), ('asip', 'oidc.idp.clogin.att.com', 0.06014227204138823), ('asip', 'services.att.com', 0.06014227204138823), ('asip', 'att.inq.com', 0.05151972407846519), ('asip', 'smetrics.att.com', 0.042897176115542134), ('asip', 'signin-static-js.att.com', 0.02134080620823453), ('asip', 'tchosted.att.com', 0.019185169217503767), ('asip', 'attservicesinc.tt.omtrdc.net', 0.019185169217503767), ('asip', 'm.att.com', 0.01702953222677301), ('asip', 'att-wireless.official-coupons.com', 0.01702953222677301), ('asip', 'geolink-igw.cloud.att.com', 0.01271825824531149), ('asip', 'chclm.att.com', 0.010562621254580727), ('asip', 'sli.tomsguide.com', 0.010562621254580727), ('asip', 'cobrowse-att.inq.com', 0.008406984263849967), ('asip', 'www.cricketwireless.com', 0.008406984263849967), ('asip', '0.ecom.attccc.com', 0.006251347273119206), ('asip', 'www.tomsguide.com', 0.006251347273119206), ('asip', 'r3.whistleout.com', 0.006251347273119206), ('asip', 'www.whistleout.com', 0.006251347273119206), ('asip', 'tomsguide.com', 0.0019400732916576848), ('asip', 'sentitlement2.mobile.att.net', 0.0019400732916576848), ('asip', 'signin-static-mjs.att.com', 0.0019400732916576848), ('asip', 'att-internet.official-coupons.com', 0.0019400732916576848), ('asip', 'cloauth.idp.clogin.att.com', 0.0019400732916576848), ('asip', 'att.com', 0.0019400732916576848), ('asip', 'hawk.tomsguide.com', 0.0019400732916576848);
"""

"""
Yournavi: 
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('yournavi', 'app.yournavi.com', 0.8252592697826395), ('yournavi', 'stats.g.doubleclick.net', 0.2385282000284131), ('yournavi', 'fonts.gstatic.com', 0.23000426196902965), ('yournavi', 'assets.website-files.com', 0.14902685040488706), ('yournavi', 'r3.o.lencr.org', 0.1419235686887342), ('yournavi', 'www.yournavi.com', 0.13908225600227303), ('yournavi', 't.co', 0.12771700525642848), ('yournavi', 'analytics.twitter.com', 0.12771700525642848), ('yournavi', 'dpm.demdex.net', 0.12771700525642848), ('yournavi', 'in.hotjar.com', 0.12061372354027561), ('yournavi', 'ocsp.sectigo.com', 0.11351044182412275), ('yournavi', 'ocsp.digicert.com', 0.11066912913766157), ('yournavi', 'images.yournavi.com', 0.10924847279443102), ('yournavi', 'api.amplitude.com', 0.10214519107827816), ('yournavi', 'unpkg.com', 0.09930387839181702), ('yournavi', 'static.ads-twitter.com', 0.09362125301889472), ('yournavi', 'pnapi.invoca.net', 0.08935928398920301), ('yournavi', 's.adroll.com', 0.07941468958658901), ('yournavi', 'static.hotjar.com', 0.079414689586589), ('yournavi', 'ext.yournavi.com', 0.069470095183975), ('yournavi', 'yournavi.com', 0.025429748543827246), ('yournavi', 'sli.tomsguide.com', 0.006961216081829805), ('yournavi', 'www.whistleout.com', 0.004119903395368661), ('yournavi', 'r3.whistleout.com', 0.004119903395368661), ('yournavi', 'www.tomsguide.com', 0.004119903395368661), ('yournavi', 'tomsguide.com', 0.0012785907089075154), ('yournavi', 'hawk.tomsguide.com', 0.0012785907089075154);
"""

"""
TSIP:
DROP TABLE ihq_prd_usertbls.intent_pattern_weights;
CREATE TABLE ihq_prd_usertbls.intent_pattern_weights VALUES ('tsip', 'www.t-mobile.com', 1.0), ('tsip', 'smetrics.t-mobile.com', 0.8463634906214929), ('tsip', 'casi.t-mobile.com', 0.8367445091647516), ('tsip', 'dpm.demdex.net', 0.5652754769411638), ('tsip', 'assets.adobedtm.com', 0.36862074493667496), ('tsip', 'speedtest.t-mobile.com', 0.3125100197723507), ('tsip', 'incoming.telemetry.mozilla.org', 0.22593918666167898), ('tsip', 'brass.account.t-mobile.com', 0.21311387805269058), ('tsip', 'r3.o.lencr.org', 0.20509806017207283), ('tsip', 'pnapi.invoca.net', 0.1970822422914551), ('tsip', 'app-measurement.com', 0.1586063164644899), ('tsip', 'mcias-va7.cloud.adobe.io', 0.1308181478116817), ('tsip', 'tmobile-app.quantummetric.com', 0.1308181478116817), ('tsip', 'tmobile-mkt-prod1-lb.campaign.adobe.com', 0.11585528776786189), ('tsip', 'tmobile.demdex.net', 0.11451931812109227), ('tsip', 'zn9vfkwwyruvt6oo1-tmobilecx.siteintercept.qualtrics.com', 0.10677069416982843), ('tsip', 'cdn.tmobile.com', 0.1040987548762892), ('tsip', 'bat.bing.com', 0.09608293699567143), ('tsip', 'tmobile.tt.omtrdc.net', 0.08539517982151444), ('tsip', 'ocsp.digicert.com', 0.08347138353016616), ('tsip', 'geolocation.onetrust.com', 0.08325762838668303), ('tsip', 'tmobile-sync.quantummetric.com', 0.07737936194089669), ('tsip', 'cdn.quantummetric.com', 0.07203548335381818), ('tsip', 'sgtm.t-mobile.com', 0.05039277507615027), ('tsip', 't-mobile.scene7.com', 0.04798802971196494), ('tsip', 'mov.t-mobile.com', 0.04558328434777961), ('tsip', 'secure.message.t-mobile.com', 0.035964302891038316), ('tsip', 'appd-geo.geo.t-mobile.com', 0.023406188211403833), ('tsip', 'cdn.styleguide.t-mobile.com', 0.022337412493988133), ('tsip', 'unav.t-mobile.com', 0.018596697483033182), ('tsip', 'tools.t-mobile.com', 0.005504194944690856), ('tsip', 'www.mintmobile.com', 0.0020306738630898303), ('tsip', 'sli.tomsguide.com', 0.001229092075028055), ('tsip', 't-mobile.com', 0.00096189814567413), ('tsip', 'www.whistleout.com', 0.0006947042163202051), ('tsip', 'r3.whistleout.com', 0.0006947042163202051), ('tsip', 'www.tomsguide.com', 0.0006947042163202051), ('tsip', 'cdn.mintmobile.com', 0.000160316357612355), ('tsip', 'vf.mintmobile.com', 0.000160316357612355), ('tsip', 'assets.mintmobile.com', 0.000160316357612355), ('tsip', 'contentkit.t-mobile.com', 0.000160316357612355), ('tsip', 't-mobile.7eer.net', 0.000160316357612355), ('tsip', 'www.t-mobilesavings.com', 0.000160316357612355), ('tsip', 'tomsguide.com', 0.000160316357612355), ('tsip', 'hawk.tomsguide.com', 0.000160316357612355), ('tsip', 'mint-mobile.58dp.net', 0.000160316357612355);
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Scoring (implicit noise removal)

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

!? TODO convert greater than conditionals on months years days to be "in set" operations instead of "greater/less than" operations. Hive applies ordering to strings based on left most string character so for example month string "12" is less than month string "5"

# COMMAND ----------

print(f"\n------ subset weblogs\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host in (select distinct(http_host) as http_host from ihq_prd_usertbls.intent_pattern_weights where intent_pattern_tag = 'tmo_switch') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")
print(f"\n------ marginal sample (ie. per line_id, lines common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")
print(f"\n------ the population (ie. across all line_ids, hosts common denominator) daily and weekly\nDROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")
print(f"\n------ tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw)\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")
print(f"\n------ raw intent pattern scores\nDROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join ihq_prd_usertbls.intent_pattern_weights b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, '{intention_pattern_name}' as intent_pattern_tag, e.week, 'weekly_0_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")
print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores select g.line_id, g.intent_pattern_tag, g.week as week_thru, g.version, g.score/f.sixteen_tile_score as score, g.qload_dt as upload_dt from g cross join f;\n")

# COMMAND ----------

print(f"------lines with 5of6 green dots\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ) select count(distinct(b.line_id)) from b where b.count_distinct_host >=5 and b.day = '{score_period}';")

# COMMAND ----------

# MAGIC %md
# MAGIC # QA

# COMMAND ----------

# print(f"\n------ subset, scale, and insert into final table\nwith b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c )")

# COMMAND ----------

------ primary lines only
with a as (select distinct(c.line_id) as line_id from ihq_prd_usertbls.intent_pattern_weekly_line_scores c inner join (select d.line_id from ihq_prd_allvm.ihq_mvp_kvp_v d where d.attr_nm_val like "%PRIMARY_LINE=Y%") d on c.line_id=d.line_id), b as (select a.line_id, b.week_thru from a inner join ihq_prd_usertbls.intent_pattern_weekly_line_scores b on a.line_id=b.line_id) select count(distinct(b.line_id)) as count from b;


# COMMAND ----------

print(f"select distinct(week_thru) from ihq_prd_usertbls.intent_pattern_weekly_line_scores;")
print(f"select week_thru, count(*) from ihq_prd_usertbls.intent_pattern_weekly_line_scores group by week_thru order by week_thru asc;")

# COMMAND ----------

# MAGIC %md
# MAGIC # each table broken into blocks

# COMMAND ----------

# part 1
# ------------------------- final table creation and scoring yyyymmdd_start, yyyymmdd_end+relativedelta(days=1)
# (year >= '{year_start}' AND month >= '{month_start}' AND day >= '{day_start}') AND (year <= '{year_end}' AND month <= '{month_end}' AND day <= '{day_end}')) a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE http_host like '%speedtest.t-mobile.com%' AND (year >= '{year_start}' AND month >= '{month_start}' AND day >= '{day_start}') AND (year <= '{year_end}' AND month <= '{month_end}' AND day <= '{day_end}')) b;")
# DROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS 
# INSERT INTO ihq_prd_usertbls.intent_pattern_weekly 
print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_weekly;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly AS SELECT a.line_id, a.year, a.month, a.day, a.hour, a.http_host FROM (SELECT a.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v a WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host in (select distinct(http_host) as http_host from ihq_prd_usertbls.intent_pattern_weights where intent_pattern_tag = 'tmo_switch') and date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}') a;\nINSERT INTO ihq_prd_usertbls.intent_pattern_weekly SELECT b.line_id, b.year, b.month, b.day, b.hour, \"speedtest.t-mobile.com\" as http_host FROM (SELECT b.* FROM ihq_prd_allvm.cust_inet_brwsng_new_v b WHERE year >= '{year_start}' and month >= '{month_start}' AND http_host like '%speedtest.t-mobile.com%') b WHERE date_time >= '{yyyymmdd_start}' AND date_time < '{yyyymmdd_end+relativedelta(days=1)}';")

# COMMAND ----------

# part 2
# -------------- rollup tables
# ------the sample (ie. per line_id, lines common denominator) daily and weekly
print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_line_scores\n(line_id string,\nhttp_host string,\nday string,\ncount_lhd bigint,\ncount_ld bigint,\ncountd_ld bigint);\nwith a as (select line_id, http_host, day, count(*) as count_lhd from ihq_prd_usertbls.intent_pattern_weekly group by line_id, http_host, day), b as (select line_id, day, count(*) as count_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), c as (select line_id, day, count(distinct(http_host)) as countd_ld from ihq_prd_usertbls.intent_pattern_weekly group by line_id, day), d as (select a.line_id, a.http_host, a.day, a.count_lhd, b.count_ld from a inner join b on a.line_id=b.line_id and a.day=b.day), e as (select d.line_id, d.http_host, d.day, d.count_lhd, d.count_ld, c.countd_ld from d inner join c on d.line_id=c.line_id and d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select e.line_id, e.http_host, e.day, e.count_lhd, e.count_ld, e.countd_ld from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_line_scores select line_id, http_host, '{score_period}' as day, sum(count_lhd) as count_lhd, sum(count_ld) as count_ld, sum(countd_ld) as countd_ld from ihq_prd_usertbls.intent_pattern_line_scores group by line_id, http_host;")


# COMMAND ----------

# part 3
# ------the population (ie. across all line_ids, hosts common denominator) daily and weekly
print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_day_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_day_scores\n(day string,\nhttp_host string,\ncount_hd bigint,\ncount_d bigint,\ncountd_d bigint);\nwith a as (select day, http_host, count(*) as count_hd from ihq_prd_usertbls.intent_pattern_weekly group by day, http_host), b as (select day, count(*) as count_d from ihq_prd_usertbls.intent_pattern_weekly group by day), c as (select day, count(distinct(http_host)) as countd_d from ihq_prd_usertbls.intent_pattern_weekly group by day), d as (select a.day, a.http_host, a.count_hd, b.count_d from a inner join b on a.day=b.day), e as (select d.day, d.http_host, d.count_hd, d.count_d, c.countd_d from d inner join c on d.day=c.day) INSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select e.day, e.http_host, e.count_hd, e.count_d, e.countd_d from e;\nINSERT INTO ihq_prd_usertbls.intent_pattern_day_scores select '{score_period}' as day, http_host, sum(count_hd) as count_hd, sum(count_d) as count_d, sum(countd_d) as countd_d from ihq_prd_usertbls.intent_pattern_day_scores group by http_host;")

# COMMAND ----------

# part 4
# ------tfidfs = (c_p_lhw/cd_p_lw)*(cd_p_w/c_p_hw) 
print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf\n(line_id string,\nweek string,\nhttp_host string,\ntf double,\nidf double,\ntfidf double);\nwith tf_table as (select line_id, '{score_period}' as week, http_host, count_lhd/countd_ld as tf from ihq_prd_usertbls.intent_pattern_line_scores where day = '{score_period}'), idf_table as (select '{score_period}' as week, http_host, countd_d/count_hd as idf from ihq_prd_usertbls.intent_pattern_day_scores where day = '{score_period}'), tfidf_table as (select tf_table.line_id, tf_table.week, tf_table.http_host, tf_table.tf, idf_table.idf, tf_table.tf*idf_table.idf as tfidf from tf_table inner join idf_table on tf_table.http_host=idf_table.http_host and tf_table.week = idf_table.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_tfidf select * from tfidf_table;")

# COMMAND ----------

# part 5
# --------------- raw intent pattern scores
print(f"DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\nWITH c AS (select a.line_id, a.week, a.http_host, a.tfidf, b.intent_pattern_tag, b.weight from ihq_prd_usertbls.intent_pattern_weekly_tfidf a inner join ihq_prd_usertbls.intent_pattern_weights b on a.http_host=b.http_host), d AS (select c.line_id, c.http_host, c.intent_pattern_tag, c.week, c.tfidf*c.weight as http_host_score from c), e AS (select d.line_id, d.intent_pattern_tag, d.week, avg(d.http_host_score) as score from d group by d.line_id, d.intent_pattern_tag, d.week) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw select e.line_id, 'TSIP' as intent_pattern_tag, e.week, 'weekly_0_0' as version, e.score, '{datetime_now_str}' as qload_dt from e;")

# COMMAND ----------

# part 6 
# ------ subset, scale, and insert into final table
# DROP TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores;\nCREATE TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores (line_id string,\nintent_pattern_tag string,\nweek string,\nversion string,\nscore double,\nqload_dt string);\n
print(f"with b as (select a.day, a.line_id, a.count_distinct_host from (select day, line_id, count(distinct(http_host)) count_distinct_host from ihq_prd_usertbls.intent_pattern_line_scores where http_host in ('smetrics.t-mobile.com','tmobile.demdex.net','www.t-mobile.com','casi.t-mobile.com', 'brass.account.t-mobile.com', 'speedtest.t-mobile.com') group by line_id, day) a ), d as (select c.* from (select c.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw c inner join b on b.line_id=c.line_id and b.day=c.week where b.count_distinct_host >=5 and b.day = '{score_period}') c ), e as (select percentile_approx(d.score, 0.0015) as left_point_15_centile_score from d group by d.week, d.version), f as (select percentile_approx(d.score, 0.16) as sixteen_tile_score from d group by d.week, d.version), g as (select ipwlsr.* from ihq_prd_usertbls.intent_pattern_weekly_line_scores_raw ipwlsr cross join e where e.left_point_15_centile_score <= ipwlsr.score) INSERT INTO TABLE ihq_prd_usertbls.intent_pattern_weekly_line_scores select g.line_id, g.intent_pattern_tag, g.week, g.version, g.score/f.sixteen_tile_score as score, g.qload_dt from g cross join f;")

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
INSERT OVERWRITE DIRECTORY '/user/svc-omg_ihq_pld/odbc_weekly_line_scores' STORED AS PARQUET SELECT * FROM ihq_prd_usertbls.intent_pattern_weekly_line_scores;
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
# MAGIC select max(report_month) from vz_feeds.mvp
