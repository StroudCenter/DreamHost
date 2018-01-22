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
from dh_dbinfo import dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Turn off chained assignment warning.
pd.options.mode.chained_assignment = None  # default='warn'


def convert_rtc_time_to_python(logger_time, timezone):
    """
    This function converts an arduino logger time into a time-zone aware python date-time object.
    Arduino's internal clocks (Real Time Clock (RTC) modules like the DS3231 chip)
    are converted to unix time by adding 946684800 - This moves the epoch time from January 1, 2000 as used by
    the RTC module to January 1, 1960 as used in Unix and other systems.
    :param logger_time: An timestamp in seconds since January 1, 2000
    :param timezone: a pytz timezone object
    :return: returns a time-zone aware python date time object
    """
    unix_time = logger_time + 946684800
    datetime_unaware = datetime.datetime.utcfromtimestamp(unix_time)
    datetime_aware = timezone.localize(datetime_unaware)
    return datetime_aware


def convert_python_time_to_rtc(py_datetime, timezone):
    """
    This is the reverse of convert_rtc_time_to_python
    :param py_datetime: A python time-zone aware datetime object
    :param timezone: the timezone of the arduino/RTC
    :return: an interger of seconds since January 1, 2000 in the RTC's timezone
    """
    datetime_aware = py_datetime.astimezone(timezone)
    unix_time = (datetime_aware - timezone.localize(datetime.datetime(1970, 1, 1))).total_seconds()
    sec_from_rtc_epoch = unix_time - 946684800
    return sec_from_rtc_epoch


def get_dreamhost_data(required_column=None, query_start=None, query_end=None,
                       data_table_name=None, data_column_name=None, debug=False):
    """
    Gets a list of all the series to append data to
    :arguments:
    required_column = A string column name which must not be blank in the query
    query_start = A datetime string which data must be newer than, defaults to none.
    query_end = A datetime string which data must be older than, defaults to none.
    dataTableName = A string table name, if data from only one is desired.
    dataColumnName = A string column name, if data from only one is desired
    :return:
    Returns a list of series.
    """

    # Set up an min and max time for when those values are not given
    if query_start is None:
        query_start = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
    if query_end is None:
        query_end = datetime.datetime.now(pytz.timezone('Etc/GMT+5')) + datetime.timedelta(days=1)

    str1 = " AND (DateTimeSeriesStart is NULL OR DateTimeSeriesStart < '" \
        + str(query_start.strftime("%Y-%m-%d %H:%M:%S")) \
        + "')"
    str2 = " AND (DateTimeSeriesEnd is NULL OR DateTimeSeriesEnd > '" \
        + str(query_end.strftime("%Y-%m-%d %H:%M:%S")) \
        + "')"
    str3 = ""
    if data_table_name is not None:
        str3 = " AND TableName = '" + data_table_name + "' "
    str4 = ""
    if data_column_name is not None:
        str4 = " AND TableColumnName = '" + data_column_name + "' "

    # Look for Data series that have an associated Aquarius Time Series ID
    query_text = \
        "SELECT DISTINCT *" \
        " FROM Series_for_midStream" \
        " WHERE " + required_column + " is not NULL " + str1 + str2 + str3 + str4 + \
        " ;"

    if debug:
        print "Timeseries selected using the query:"
        print query_text

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
    cur = conn.cursor()

    # Create a pandas data frame from the query
    series_table = pd.read_sql(query_text, conn)

    site_query = \
        "SELECT DISTINCT SiteID, AQLocationID, EnviroDIYToken, SamplingFeatureGUID" \
        " FROM Sites_for_midStream" \
        " WHERE AQLocationID is not NULL OR EnviroDIYToken is not NULL"
    sites = pd.read_sql(site_query, conn)
    sites['AQLocationID'] = sites['AQLocationID'].fillna(0).astype('int64')

    series_table = series_table.merge(sites, on="SiteID")

    if debug:
        print "which returns %s series" % len(series_table.index)

    # Fill in any missing time zones with '-5'
    series_table['SeriesTimeZone'].fillna(value=-5, inplace=True)

    # create a series/column with the string timezone name
    series_table['utc_offset_string'] = 'Etc/GMT+' + (-1*series_table['SeriesTimeZone']).apply(str).str.strip('.0')
    series_table['utc_offset_string'].str.replace('Etc/GMT+-', 'Etc/GMT-')

    # Fix the types of the date/time columns
    series_table['DateTimeSeriesStart'] = pd.to_datetime(series_table['DateTimeSeriesStart'])
    series_table['DateTimeSeriesEnd'] = pd.to_datetime(series_table['DateTimeSeriesEnd'])

    # Set up an min and max time for when those values are NULL in dreamhost
    series_table['DateTimeSeriesStart'].fillna(np.datetime64('2000-01-01T00:00:00'))
    series_table['DateTimeSeriesEnd'].fillna(np.datetime64('now') + np.timedelta64(1, 'D'))

    # Localize the datetime columns based on the timezone name
    series_table['DateTimeSeriesStart'] = \
        series_table.apply(lambda row1: pytz.timezone(row1.utc_offset_string).localize(row1.DateTimeSeriesStart),
                           axis=1)
    series_table['DateTimeSeriesEnd'] = \
        series_table.apply(lambda row1: pytz.timezone(row1.utc_offset_string).localize(row1.DateTimeSeriesEnd),
                           axis=1)

    # Verify the actual date and time to pick from the dream host tables
    series_table['DateTimeQueryStart'] = series_table.apply(lambda row1:
                                                            max(query_start, row1.DateTimeSeriesStart), axis=1)
    series_table['DateTimeQueryEnd'] = series_table.apply(lambda row1:
                                                          min(query_end, row1.DateTimeSeriesEnd), axis=1)

    # Get the actual data for each series

    # Create a new table to append data to
    series_table_with_data = series_table

    for (idx, row) in series_table.iterrows():
        data_dt = get_data_from_dreamhost_table(table=row.TableName, column=row.TableColumnName,
                                                start_dt=row.DateTimeQueryStart, end_dt=row.DateTimeQueryEnd,
                                                debug=debug)
        series_table.loc[idx, 'NumberDataValues'] = len(data_dt.index)
        if len(data_dt.index) > 0:
            data_dt['SeriesID'] = row.SeriesID

            series_table_with_data = series_table_with_data.merge(data_dt, how='outer', on='SeriesID')
            if 'data_value_x' in series_table_with_data:
                series_table_with_data['data_value'] = \
                    series_table_with_data['data_value_x'].fillna(series_table_with_data['data_value_y'])
                series_table_with_data.drop('data_value_x', axis=1, inplace=True)
                series_table_with_data.drop('data_value_y', axis=1, inplace=True)

                series_table_with_data['timestamp'] = \
                    series_table_with_data['timestamp_x'].fillna(series_table_with_data['timestamp_y'])
                series_table_with_data.drop('timestamp_x', axis=1, inplace=True)
                series_table_with_data.drop('timestamp_y', axis=1, inplace=True)


    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    # Check number of values returned
    series_table_with_data["NumberDataValues"] = \
        series_table_with_data.groupby(["SeriesID"])['data_value'].transform('count')
    # Remove rows where no values were returned
    series_table_with_data = \
        series_table_with_data.drop(series_table_with_data[series_table_with_data.NumberDataValues == 0].index)
    series_table = \
        series_table.drop(series_table[series_table.NumberDataValues == 0].index)

    return series_table, series_table_with_data


def get_data_from_dreamhost_table(table, column, start_dt=None, end_dt=None, debug=False):
    """
    Returns a pandas data frame with the timestamp and data value from a given table and column.
    :param table: A string which is the same of the SQL table of interest
    :param column: A string which is the name of the column of interest
    :param start_dt: The first date/time for data - All date times should be timezone AWARE
    :param end_dt: The last date/time for data
    :param debug: A boolean for whether extra print commands apply
    :return: A pandas data frame with the timestamp and data value from a given table and column.
    """

    # Set up an min and max time for when those values are not given
    if start_dt is None:
        start_dt = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
    if end_dt is None:
        end_dt = datetime.datetime.now(pytz.timezone('Etc/GMT+5')) + datetime.timedelta(days=1)

    start_tz = start_dt.tzinfo
    end_tz = end_dt.tzinfo

    if table in ["davis", "CRDavis"]:
        # The meteobridges streaming this data stream a column of time in UTC
        dt_col = "mbutcdatetime"
        sql_start = start_dt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql_end = end_dt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt_col = "Loggertime"
        sql_start = convert_python_time_to_rtc(start_dt, start_tz)
        sql_end = convert_python_time_to_rtc(end_dt, end_tz)

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    query_text = "SELECT DISTINCT " + dt_col + ", " + column + " as data_value" \
                 + " FROM " + table  \
                 + " WHERE " + column + " IS NOT NULL" \
                 + " AND " + dt_col + " >= '" + str(sql_start) + "'" \
                 + " AND " + dt_col + " <= '" + str(sql_end) + "'" \
                 + " ORDER BY " + dt_col \
                 + " ;"

    if debug:
        print "   Data selected using the query:"
        print "   " + query_text
    t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)

    values_table = pd.read_sql(query_text, conn)

    # Close out the database connections
    conn.close()  # close the database connection

    if debug:
        print "   which returns %s values" % (len(values_table))
        t2 = datetime.datetime.now()
        print "   SQL execution took %s" % (t2 - t1)

    # Create a new column with a proper uniform python datetime data type
    # remove old datetime column
    if len(values_table) > 0:
        if table in ["davis", "CRDavis"]:
            values_table['timestamp'] = \
                values_table.apply(lambda row1: pytz.utc.localize(row1.mbutcdatetime).astimezone(start_tz), axis=1)
            values_table.drop(dt_col, axis=1, inplace=True)
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp'] = np.vectorize(convert_rtc_time_to_python)(values_table[dt_col], start_tz)
            values_table.drop(dt_col, axis=1, inplace=True)

        if debug:
            print "The first and last rows from DreamHost:\r\n", values_table.head(2), "\r\n", values_table.tail(2)

    return values_table