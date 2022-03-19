import pymysql
import pandas as pd

creds = {
    "host": "54.69.58.20",
    "user": "wfsuser",
    "db": "RRF_TABLES_TEST",
    "passwd": "REConnect4321!?",
}

conn = pymysql.connect(**creds)


# OLD TO NEW TABLE VALIDATION

pro_df = pd.read_sql(
    """SELECT DISTINCT SUBSTATION_ID, SOURCE_TAG FROM DATA_ACTUAL_PSS_PRO_2020 WHERE TIMESTAMP BETWEEN '2020-03-03' AND '2020-03-04';""",
    conn,
)

solar_df = pd.read_sql(
    "SELECT DISTINCT SUBSTATION_ID FROM RSM_SUBSTATION_MASTER WHERE ENERGY_TYPE='SOLAR'",
    conn,
)

raw_solar_missing = pd.merge(
    solar_df, pro_df, on=['SUBSTATION_ID'], how='left')

raw_solar_missing.to_csv("SOLAR_SUBSTATIONS_MISSING.csv", index=False)

solar_df_old_di = pd.read_sql(
    """SELECT DISTINCT SUBSTATION_ID FROM DATA_SOLAR_ACTUAL_SUBSTATION WHERE TIME_STAMP BETWEEN '2020-03-03' AND '2020-03-04';""",
    conn,
)

actual_solar_missing = pd.merge(solar_df_old_di, raw_solar_missing, on=[
                                'SUBSTATION_ID'], how='left')

# 3 aggregation substations missing
actual_solar_missing.to_csv("SOLAR_SUBSTATIONS_MISSING2.csv", index=False)

wind_df = pd.read_sql(
    "SELECT DISTINCT SUBSTATION_ID FROM RSM_SUBSTATION_MASTER WHERE ENERGY_TYPE='WIND'",
    conn,
)

raw_wind_missing = pd.merge(wind_df, pro_df, on=['SUBSTATION_ID'], how='left')

raw_wind_missing.to_csv('WIND_SUBSTATIONS_MISSNG.csv', index=False)

wind_df_old_di = pd.read_sql(
    """SELECT DISTINCT SUBSTATION_ID FROM DATA_WIND_ACTUAL_SUBSTATION WHERE TIMESTAMP BETWEEN '2020-03-03' AND '2020-03-04';""",
    conn,
)

actual_wind_missing = pd.merge(wind_df_old_di, raw_wind_missing, on=[
                               'SUBSTATION_ID'], how='left')

# all fine for real time
actual_wind_missing.to_csv("WIND_SUBSTATIONS_MISSING2.csv", index=False)


# historical_new_di=pd.read_sql("""SELECT DATE(TIMESTAMP), SOURCE_TAG, COUNT( distinct SUBSTATION_ID) FROM DATA_ACTUAL_PSS_PRO_2020
# WHERE TIMESTAMP BETWEEN '2020-01-01' AND '2020-03-03'
# GROUP BY DATE(TIMESTAMP), SOURCE_TAG;""", conn)

historical_new_di = pd.read_sql("""SELECT DATE(TIMESTAMP) as DAY, COUNT( distinct SUBSTATION_ID) as total_ss
FROM DATA_ACTUAL_PSS_PRO_2020 
WHERE TIMESTAMP BETWEEN '2020-01-01' AND '2020-03-03'
GROUP BY DATE(TIMESTAMP);""", conn)

historical_old_solar_di = pd.read_sql("""SELECT DATE(TIME_STAMP) as DAY, COUNT( distinct SUBSTATION_ID) 
	as solar_ss FROM DATA_SOLAR_ACTUAL_SUBSTATION 
WHERE TIME_STAMP BETWEEN '2020-01-01' AND '2020-03-03'
GROUP BY DATE(TIME_STAMP);""", conn)

historical_old_wind_di = pd.read_sql("""SELECT DATE(TIMESTAMP) as DAY, COUNT( distinct SUBSTATION_ID) as wind_ss 
	FROM DATA_WIND_ACTUAL_SUBSTATION
WHERE TIMESTAMP BETWEEN '2020-01-01' AND '2020-03-03'
GROUP BY DATE(TIMESTAMP);""", conn)

historical_combined = pd.merge(
    historical_old_wind_di, historical_old_solar_di, on=['DAY'], how='outer')

historical_combined['total_ss'] = historical_combined['solar_ss'] + \
    historical_combined['wind_ss']

historical_verification = pd.merge(
    historical_combined, historical_new_di, on=['DAY'], how='outer')

historical_verification.to_csv('NUMERICAL_VERIFICATION_OF_SS.csv', index=False)


historical_ss_verify = pd.read_sql("""SELECT distinct SUBSTATION_ID,SOURCE_TAG FROM DATA_ACTUAL_PSS_PRO_2020 
WHERE TIMESTAMP BETWEEN '2020-01-01' AND '2020-03-03';""", conn)


df1 = pd.read_sql("""SELECT distinct SUBSTATION_ID FROM DATA_ACTUAL_PSS_PRO_2020 
WHERE TIMESTAMP BETWEEN '2020-03-03 00:00:00' AND '2020-03-03 23:45:00';""", conn)

df2 = pd.read_sql("""SELECT distinct SUBSTATION_ID FROM DATA_SOLAR_ACTUAL_SUBSTATION 
WHERE TIME_STAMP BETWEEN '2020-03-03' AND '2020-03-03 23:45:00';""", conn)

df3 = pd.read_sql("""SELECT distinct SUBSTATION_ID FROM DATA_WIND_ACTUAL_SUBSTATION 
WHERE TIMESTAMP BETWEEN '2020-01-01' AND '2020-01-01 23:45:00';""", conn)

df2 = df2.append(df3)

verify_ss_di = pd.merge(df1, df2, on=['SUBSTATION_ID'], how='outer')


# RAW TO PRO VALIDATION

# 1) WTG RAW TO PSS_PRO

turbine_ids = pd.read_sql("""SELECT TURBINE_ID, TIMESTAMP as r_time, ATTRIBUTE_1 as r_power, UPLOAD_TIMESTAMP as r_up_ts
    FROM DATA_ACTUAL_WTG_RAW_2020 WHERE TIMESTAMP BETWEEN '2020-03-16' AND '2020-03-16 00:14:59';""", conn)

sub_turb_map = pd.read_sql(
    """SELECT SUBSTATION_ID, TURBINE_ID FROM RSM_SUBSTATION_TURBINE WHERE SUBSTATION_ID NOT IN 
    (SELECT SUBSTATION_ID FROM RSM_SUBSTATION_MASTER WHERE DATA_SCADA_DATA_MODE = 0 OR DATA_SCADA_DATA_MODE=2);""", conn)

raw_tid_sub_map = pd.merge(turbine_ids, sub_turb_map, on=[
                             'TURBINE_ID'], how='left')

raw_tid_sub_map.to_csv('r_tid_sub_map.csv',index=False)


agg_raw=raw_tid_sub_map.groupby(['SUBSTATION_ID', 'r_time'],as_index=False).sum()   
agg_raw.drop_duplicates(subset='SUBSTATION_ID',keep=False,inplace=True)

substations_len = len(
    drop_missing_tids['SUBSTATION_ID'].unique())  # 143 from wtg_raw

ss_num_pro = pd.read_sql("""SELECT SUBSTATION_ID,ATTRIBUTE_1 as p_power,TIMESTAMP as p_time, 
    UPLOAD_TIMESTAMP as p_up_ts FROM DATA_ACTUAL_PSS_PRO_2020 
    WHERE SOURCE_TAG='UCA_SCADA_CLT' AND TIMESTAMP BETWEEN '2020-03-16 00:00:00' AND '2020-03-16 00:14:59';""", conn)


fin_verification_r_to_p=pd.merge(agg_raw,ss_num_pro,on=['SUBSTATION_ID'],how='left')

ss_wtg_raw = list(drop_missing_tids['SUBSTATION_ID'].unique())

ss_pss_pro = list(ss_num_pro['SUBSTATION_ID'])

missing_substations_in_pro=list(set(ss_wtg_raw)-set(ss_pss_pro))

# 2) PSS_RAW TO PSS_PRO


solar_df = pd.read_sql(
    "SELECT DISTINCT SUBSTATION_ID FROM RSM_SUBSTATION_MASTER WHERE ENERGY_TYPE='SOLAR'",
    conn,
)

scada_ch=pd.read_sql("""SELECT SUBSTATION_ID, SUBSTATION_NAME, DATA_SCADA_DATA_MODE, ENERGY_TYPE FROM RSM_SUBSTATION_MASTER 
     WHERE SUBSTATION_ID IN (missing_substations_in_pro)""",conn)







METER_DAT=pd.read_sql("SELECT * FROM WF_SUBSTATION_METER_DATA WHERE TIMESTAMP BETWEEN '2020-03-12' AND '2020-03-13'",met_conn)

METER_DAT.groupby(['SUBSTATION_ID','METER_ID','TIMESTAMP'],as_index=False).sum()

METER_pro_data=pd.read_sql("""SELECT SUBSTATION_ID,TIMESTAMP,UPLOAD_TIMESTAMP,ATTRIBUTE_1 AS PRO_POWER FROM
    DATA_ACTUAL_PSS_PRO_2020 WHERE SOURCE_TAG='PSS_METER_RES' AND TIMESTAMP BETWEEN '2020-03-12' AND '2020-03-13'""",conn)

METER_pro_data.to_csv('meter_pro.csv',index=False)
meter_verification=pd.merge(METER_DAT,METER_pro_data,on=['SUBSTATION_ID'],how='inner')

# SELECT * FROM DATA_ACTUAL_WTG_RAW_2020 WHERE TURBINE_ID IN (SELECT TURBINE_ID FROM RSM_SUBSTATION_TURBINE WHERE SUBSTATION_ID=
#     'SS00002' AND TIMESTAMP BETWEEN '2020-03-16' AND '2020-03-16 00:14:59')


meter_filter=pd.read_sql("""select METER_ID FROM WF_METER_REGISTRATION WHERE
    MAKE_LIVE_STATUS=1""",met_conn)