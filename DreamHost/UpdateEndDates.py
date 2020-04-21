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

# Set up connection to the DreamHost MySQL database
conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                       user=dh_db_user, passwd=dh_db_pass)
cur = conn.cursor()

query_text = "select distinct TableName, DateTimeSeriesStart, DateTimeSeriesEnd from Series_for_midStream" + \
             " where DateTimeSeriesStart is NULL or DateTimeSeriesEnd = NULL"
tables = pd.read_sql(query_text, conn)

for index, table in tables.iterrows():
    last_date = None
    first_date = None
    try:
        new_query = "select max(Date) from %s" % table['TableName']
        cur.execute(new_query)
        last_date = cur.fetchone()[0]
        new_query = "select min(Date) from %s" % table['TableName']
        cur.execute(new_query)
        first_date = cur.fetchone()[0]
    except:
        pass
    print("{} {} ({}) -  {} ({})".format(table['TableName'], table['DateTimeSeriesStart'], first_date,
                                         table['DateTimeSeriesEnd'], last_date))

    if pd.isna(table['DateTimeSeriesStart']) and first_date is not None:
        if first_date.hour == 0:
            start_date_time = datetime.datetime(year=first_date.year, month=first_date.month, day=first_date.day - 1,
                                                hour=23, minute=0, second=0)
        else:
            start_date_time = datetime.datetime(year=first_date.year, month=first_date.month, day=first_date.day,
                                              hour=first_date.hour - 1, minute=0, second=0)
        edit_query =\
            "update Series_for_midStream set DateTimeSeriesStart = \"%s\" " \
            "where TableName = \"%s\" and DateTimeSeriesStart is NULL" % (start_date_time, table['TableName'])
        print(edit_query)
        cur.execute(edit_query)
        conn.commit()

    if pd.isna(table['DateTimeSeriesEnd']) and last_date is not None and \
            last_date < datetime.datetime.now() - datetime.timedelta(days=14):
        if last_date.hour == 23:
            end_date_time = datetime.datetime(year=last_date.year, month=last_date.month, day=last_date.day + 1,
                                          hour=0, minute=0, second=0)
        else:
            end_date_time = datetime.datetime(year=last_date.year, month=last_date.month, day=last_date.day,
                                          hour=last_date.hour + 1, minute=0, second=0)
        edit_query =\
            "update Series_for_midStream set DateTimeSeriesEnd = \"%s\" " \
            "where TableName = \"%s\" and DateTimeSeriesEnd is NULL" % (end_date_time, table[0])
        print(edit_query)
        cur.execute(edit_query)
        conn.commit()

conn.close()
