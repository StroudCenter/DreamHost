# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 5/16/2016 at 6:14 PM


"""

import pymysql
import pandas as pd
import datetime
import pytz
import numpy as np

# Bring in all of the database connection information.
# from DreamHost.dh_dbinfo import dh_db_host, dh_db_name, dh_db_name_cib, dh_db_user, dh_db_pass
from dh_dbinfo import dh_db_host, dh_db_name, dh_db_name_cib, dh_db_user, dh_db_pass

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Turn off chained assignment warning.
pd.options.mode.chained_assignment = None  # default='warn'

# %%
drwi_aq_ids = pd.read_csv("R:\WilliamPenn_Delaware River\CitSci\Data\PythonToAquarius\AquariusDatasets.csv", header=0)
drwi_aq_ids['SiteID'] = drwi_aq_ids['SiteID'].astype(str)

fromMaster = pd.read_excel("R:/WilliamPenn_Delaware River/CitSci/Data/SensorStation_DeploymentInfo_MASTER.xlsx",
                           sheet_name="Main", header=0, usecols="A:B, L:N, AV").dropna(axis=0, subset=["SiteID", 'LoggerID'])

# %%
aq_sets = fromMaster.merge(drwi_aq_ids, how='outer', on='SiteID')
aq_sets = aq_sets.drop(labels='i', axis=1)

aq_sets2 = pd.melt(aq_sets, id_vars=["LoggerID", "SiteID", 'Name', 'latitude', 'longitude',
                                     'Elevation (off google maps if necessary)', 'AquariusLocationId'],
                   value_vars=['BatterySeriesId', 'BoardTempSeriesId',
                               'ConductivitySeriesId', 'TempSeriesId', 'DepthSeriesId',
                               'TurbLowSeriesId', 'TurbHighSeriesId',
                               'SignalPercentSeriesId', 'SignalStrengthSeriesId'],
                               value_name = 'AquariusTimeSeriesId')
aq_sets['TableColumnName'] = ''
aq_sets['VariableID'] = 0
aq_sets2.loc[aq_sets2['variable'] == 'BatterySeriesId', 'TableColumnName'] = 'Battery'
aq_sets2.loc[aq_sets2['variable'] == 'BoardTempSeriesId', 'TableColumnName'] = 'BoardTemp'
aq_sets2.loc[aq_sets2['variable'] == 'ConductivitySeriesId', 'TableColumnName'] = 'CTDcond'
aq_sets2.loc[aq_sets2['variable'] == 'TempSeriesId', 'TableColumnName'] = 'CTDtemp'
aq_sets2.loc[aq_sets2['variable'] == 'DepthSeriesId', 'TableColumnName'] = 'CTDdepth'
aq_sets2.loc[aq_sets2['variable'] == 'TurbLowSeriesId', 'TableColumnName'] = 'TurbLow'
aq_sets2.loc[aq_sets2['variable'] == 'TurbHighSeriesId', 'TableColumnName'] = 'TurbHigh'
aq_sets2.loc[aq_sets2['variable'] == 'SignalPercentSeriesId', 'TableColumnName'] = 'signalPercent'
aq_sets2.loc[aq_sets2['variable'] == 'SignalStrengthSeriesId', 'TableColumnName'] = 'RSSI'
aq_sets2.loc[aq_sets2['variable'] == 'DOppmSeriesId', 'TableColumnName'] = 'DOppm'
aq_sets2.loc[aq_sets2['variable'] == 'DOpctSeriesId', 'TableColumnName'] = 'DOpercent'
aq_sets2.loc[aq_sets2['variable'] == 'DOtempSeriesId', 'TableColumnName'] = 'DOtempC'

aq_sets2.loc[aq_sets2['variable'] == 'BatterySeriesId', 'VariableID'] = 16
aq_sets2.loc[aq_sets2['variable'] == 'BoardTempSeriesId', 'VariableID'] = 6
aq_sets2.loc[aq_sets2['variable'] == 'ConductivitySeriesId', 'VariableID'] = 3
aq_sets2.loc[aq_sets2['variable'] == 'TempSeriesId', 'VariableID'] = 2
aq_sets2.loc[aq_sets2['variable'] == 'DepthSeriesId', 'VariableID'] = 1
aq_sets2.loc[aq_sets2['variable'] == 'TurbLowSeriesId', 'VariableID'] = 4
aq_sets2.loc[aq_sets2['variable'] == 'TurbHighSeriesId', 'VariableID'] = 5
aq_sets2.loc[aq_sets2['variable'] == 'DOppmSeriesId', 'VariableID'] = 37
aq_sets2.loc[aq_sets2['variable'] == 'DOpctSeriesId', 'VariableID'] = 36
aq_sets2.loc[aq_sets2['variable'] == 'DOtempSeriesId', 'VariableID'] = 38
aq_sets2.loc[aq_sets2['variable'] == 'SignalPercentSeriesId', 'VariableID'] = 77
aq_sets2.loc[aq_sets2['variable'] == 'SignalStrengthSeriesId', 'VariableID'] = 78
aq_sets3 = aq_sets2.loc[aq_sets2['AquariusTimeSeriesId'] > 0]

# %%
# Set up connection to the DreamHost MySQL database
conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                       user=dh_db_user, passwd=dh_db_pass)
cur = conn.cursor()

query = "select distinct SiteID as SiteNumber, SiteCode " \
        " from Sites_for_midStream "

dh_sites = pd.read_sql(query, conn)
dh_sites['SiteCode'] = dh_sites['SiteCode'].astype(str)
dh_sites['SiteNumber'] = dh_sites['SiteNumber'].astype(int)

aq_with_site_num = pd.merge(left=aq_sets3, right=dh_sites, left_on='SiteID', right_on='SiteCode', how='left')
aq_with_site_num = aq_with_site_num.drop(columns="SiteCode")

query = "select SeriesID, TableName, TableColumnName, VariableID, AQTimeSeriesID, " \
        "Sites_for_midStream.SiteID as SiteNumber, SiteCode, SiteName, " \
        "Latitude, Longitude, Elevation_m, AQLocationID" \
        " from Series_for_midStream " \
        " right join Sites_for_midStream" \
        " on  Series_for_midStream.SiteID = Sites_for_midStream.SiteID"

dh_series = pd.read_sql(query, conn)
dh_series['SiteCode'] = dh_series['SiteCode'].astype(str)
dh_series['SiteNumber'] = dh_series['SiteNumber'].astype(int)

# %%
combined_series = pd.merge(left=aq_with_site_num, right=dh_series,
                          left_on=['SiteNumber', 'SiteID', 'TableColumnName', 'VariableID'],
                          right_on=['SiteNumber', 'SiteCode', 'TableColumnName', 'VariableID'],
                          how='left')
combined_series = combined_series.sort_values(by=['LoggerID', 'SiteCode', 'TableColumnName'])
combined_series['Name'] = combined_series['Name'].fillna(combined_series['SiteName'])\
    .fillna(combined_series['SiteID']).fillna(combined_series['LoggerID']).fillna(0)
combined_series['latitude'] = combined_series['latitude'].fillna(combined_series['Latitude']).fillna(0)
combined_series['longitude'] = combined_series['longitude'].fillna(combined_series['Longitude']).fillna(0)
combined_series['elevation'] = combined_series['Elevation (off google maps if necessary)']\
    .fillna(combined_series['Elevation_m']).fillna(0)
combined_series['AquariusLocationId'] = combined_series['AquariusLocationId']\
    .fillna(combined_series['AQLocationID']).fillna(0)
combined_series = combined_series.drop(labels=['Latitude', 'Longitude',
                                             'Elevation (off google maps if necessary)', 'Elevation_m',
                                             'AQLocationID'], axis=1)


# %%
check = combined_series.loc[pd.notna(combined_series['AQTimeSeriesID']) & (combined_series['AQTimeSeriesID'] != combined_series['AquariusTimeSeriesId'])]

check = combined_series.loc[pd.notna(combined_series['AQTimeSeriesID']) & (combined_series['AQTimeSeriesID'] != combined_series['AquariusTimeSeriesId'])]
check2 = combined_series.loc[pd.notna(combined_series['TableName']) & (combined_series['TableName'] != combined_series['LoggerID'])]

# %%
i = 0
j = 0
for index, row in combined_series.iterrows():
    # print(row['LoggerID'], row['TableColumnName'])
    if pd.notna(row['SeriesID']):
        edit_query =\
            "update Series_for_midStream " \
            "set TableName = \"%s\" " \
            ", TableColumnName = \"%s\" " \
            ", SeriesTimeZone = -5 " \
            ", SiteID = %d " \
            ", VariableID = %d " \
            ", AQTimeSeriesID = %d " \
            "where SeriesID = %d" \
            % (row['LoggerID'], row['TableColumnName'], row['SiteNumber'], row['VariableID'],
               row['AquariusTimeSeriesId'], row['SeriesID'])
        # print(edit_query)
        i += 1
        cur.execute(edit_query)
        conn.commit()

    else:
        insert_query =\
            "insert into Series_for_midStream " \
            "(TableName, TableColumnName, SeriesTimeZone, SiteID, VariableID, AQTimeSeriesID) " \
            "values ( \"%s\", \"%s\", -5, %d, %d, \"%s\")" \
            % (row['LoggerID'], row['TableColumnName'], row['SiteNumber'], row['VariableID'],
               row['AquariusTimeSeriesId'])
        # print(insert_query)
        j += 1
        cur.execute(insert_query)
        conn.commit()

print("%s series edited" % i)
print("%s series created" % j)



# %%
conn.close()
