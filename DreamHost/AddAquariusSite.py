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
from DreamHost.dh_dbinfo import dh_db_host, dh_db_name, dh_db_name_cib, dh_db_user, dh_db_pass

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Turn off chained assignment warning.
pd.options.mode.chained_assignment = None  # default='warn'

drwi_aq_ids = pd.read_csv("R:\WilliamPenn_Delaware River\CitSci\Data\PythonToAquarius\AquariusDatasets.csv", header=0)
drwi_aq_ids['SiteID'] = drwi_aq_ids['SiteID'].astype(str)

fromMaster = pd.read_excel("R:/WilliamPenn_Delaware River/CitSci/Data/SensorStation_DeploymentInfo_MASTER.xlsx",
                           sheet_name="Main", header=0, usecols="A:B, L:N, AV").dropna(axis=0, subset=["SiteID", 'LoggerID'])

aq_sets = fromMaster.merge(drwi_aq_ids, how='outer', on='SiteID')
aq_sets = aq_sets.drop(labels='i', axis=1)

# Set up connection to the DreamHost MySQL database
conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                       user=dh_db_user, passwd=dh_db_pass)
cur = conn.cursor()

query = "select * from Sites_for_midStream"

sites_need_ids = pd.read_sql(query, conn)

sites_need_ids['SiteCode'] = sites_need_ids['SiteCode'].astype(str)
sites_need_ids = sites_need_ids.drop(labels='SiteID', axis=1)

combined_sites = pd.merge(left=aq_sets, right=sites_need_ids, left_on='SiteID', right_on='SiteCode', how='left')
combined_sites = combined_sites.sort_values(by='SiteID')
combined_sites['Name'] = combined_sites['Name'].fillna(combined_sites['SiteName']).fillna(combined_sites['SiteID']).fillna(combined_sites['LoggerID']).fillna(0)
combined_sites['latitude'] = combined_sites['latitude'].fillna(combined_sites['Latitude']).fillna(0)
combined_sites['longitude'] = combined_sites['longitude'].fillna(combined_sites['Longitude']).fillna(0)
combined_sites['elevation'] = combined_sites['Elevation (off google maps if necessary)'].fillna(combined_sites['Elevation_m']).fillna(0)
combined_sites['AquariusLocationId'] = combined_sites['AquariusLocationId'].fillna(combined_sites['AQLocationID']).fillna(0)
combined_sites = combined_sites.drop(labels=['Latitude', 'Longitude',
                                             'Elevation (off google maps if necessary)', 'Elevation_m',
                                             'AQLocationID'], axis=1)


for index, row in combined_sites.iterrows():
    print(row['SiteCode'])
    if pd.notna(row['SiteCode']):
        edit_query =\
            "update Sites_for_midStream " \
            "set SiteName = \"%s\" " \
            ", Latitude = %f " \
            ", Longitude = %f " \
            ", Elevation_m = %f " \
            ", SpatialReference = \"WGS84\"" \
            ", AQLocationID = %d " \
            "where SiteCode = \"%s\"" \
            % (row['Name'], row['latitude'], row['longitude'], row['elevation'],
               row['AquariusLocationId'], row['SiteID'])
        print(edit_query)
        cur.execute(edit_query)
        conn.commit()

    else:
        insert_query =\
            "insert into Sites_for_midStream " \
            "(SiteName, Latitude, Longitude, Elevation_m, SpatialReference, AQLocationID, SiteCode) " \
            "values ( \"%s\", %f, %f, %f, \"WGS84\", %d, \"%s\")" \
            % (row['Name'], row['latitude'], row['longitude'], row['elevation'],
               row['AquariusLocationId'], row['SiteID'])
        print(insert_query)
        cur.execute(insert_query)
        conn.commit()


conn.close()
