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
from dbinfo import dbhost, dbname, dbuser, dbpswd

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Turn off chained assignment warning.
pd.options.mode.chained_assignment = None  # default='warn'


def get_dreamhost_series(query_start=None, query_end=None, table=None, column=None, debug=False):
    """
    Gets a list of all the series to append data to
    :arguments:
    cutoff_for_recent = A datetime string to use to exclude inactive series.
        All series with and end before this time will be excluded.
        Defaults to none.
    table = A table name, if data from only one is desired.
    column = A column name, if data from only one is desired
    :return:
    Returns a list of series.
    """

    str1 = ""
    str2 = ""
    str3 = ""
    str4 = ""
    if query_start is not None:
        str1 = " AND (DateTimeSeriesStart is NULL OR DateTimeSeriesStart < '" \
            + str(query_start.strftime("%Y-%m-%d %H:%M:%S")) \
            + "')"
    if query_end is not None:
        str2 = " AND (DateTimeSeriesEnd is NULL OR DateTimeSeriesEnd > '" \
            + str(query_end.strftime("%Y-%m-%d %H:%M:%S")) \
            + "')"
    if table is not None:
        str3 = " AND TableName = '" + table + "' "
    if column is not None:
        str4 = " AND TableColumnName = '" + column + "' "

    # Look for Dataseries that have an associated Aquarius Time Series ID
    query_text = \
        "SELECT DISTINCT AQTimeSeriesID, AQLocationID, TableName, TableColumnName, SeriesTimeZone," \
        " DateTimeSeriesStart, DateTimeSeriesEnd " \
        " FROM Series_for_midStream " \
        " WHERE AQTimeSeriesID != 0 " + str1 + str2 + str3 + str4 + \
        " ORDER BY TableName, AQTimeSeriesID ;"

    if debug:
        print "Timeseries selected using the query:"
        print query_text

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dbhost, db=dbname, user=dbuser, passwd=dbpswd)
    cur = conn.cursor()

    cur.execute(query_text)

    aq_series = list(cur.fetchall())
    aq_series_list = [list(series) for series in aq_series]

    for series in aq_series_list:
        # Turn the time zone for the series into a pytz timezone
        if series[4] is not None:
            utc_offset_string = '{:+3.0f}'.format(series[4]*-1).strip()
            timezone = pytz.timezone('Etc/GMT'+utc_offset_string)
            series[4] = timezone
        else:
            series[4] = pytz.timezone('Etc/GMT+5')
        # Make the series start and end times "timezone-aware"
        # Per the database instructions, these times should always be in EST, regardless of the timezone of the logger.
        if series[5] is not None:
            series[5] = pytz.timezone('Etc/GMT+5').localize(series[5])
        if series[6] is not None:
            series[6] = pytz.timezone('Etc/GMT+5').localize(series[6])
    if debug:
        # print aq_series_list
        # print type(aq_series)
        print "which returns %s series" % len(aq_series)

    # Close out the database connections
    cur.close()  # close the database cursor
    conn.close()  # close the database connection

    return aq_series_list


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


def convert_python_time_to_rtc(pydatetime, timezone):
    """
    This is the reverse of convert_rtc_time_to_python
    :param pydatetime: A python time-zone aware datetime object
    :param timezone: the timezone of the arduino/RTC
    :return: an interger of seconds since January 1, 2000 in the RTC's timezone
    """
    datetime_aware = pydatetime.astimezone(timezone)
    unix_time = (datetime_aware - timezone.localize(datetime.datetime(1970, 1, 1))).total_seconds()
    sec_from_rtc_epoch = unix_time - 946684800
    return sec_from_rtc_epoch


def get_data_from_dreamhost_table(table, column, series_start=None, series_end=None,
                                  query_start=None, query_end=None, debug=False):
    """
    Returns a pandas data frame with the timestamp and data value from a given table and column.
    :param table: A string which is the same of the SQL table of interest
    :param column: A string which is the name of the column of interest
    :param series_start: The date/time when the series begins
    :param series_end: The date/time when the series end
    :param query_start: The beginning date/time of interest
    :param query_end: The ending date/time of interest
    :param debug: A boolean for whether extra print commands apply
    :return: A pandas data frame with the timestamp and data value from a given table and column.
    """

    # Set up an min and max time for when those values are NULL in dreamhost
    if series_start is None:
        series_start = pytz.utc.localize(datetime.datetime(2000, 1, 1, 0, 0, 0))
    if series_end is None:
        series_end = datetime.datetime.now(pytz.utc) + datetime.timedelta(days=1)  # Future times clearly not valid
    # Set up an min and max time for when those values are NULL in dreamhost
    if query_start is None:
        query_start = pytz.utc.localize(datetime.datetime(2000, 1, 1, 0, 0, 0))
    if query_end is None:
        query_end = datetime.datetime.now(pytz.utc) + datetime.timedelta(days=1)  # Future times clearly not valid

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.

    # TODO: Take into account time zones from the dreamhost table.
    if table in ["davis", "CRDavis"]:
        # The meteobridges streaming this data stream a column of time in UTC
        dt_col = "mbutcdatetime"
        sql_start = max(series_start, query_start).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql_end = min(series_end, query_end).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt_col = "Loggertime"
        sql_start_py = max(series_start, query_start).astimezone(pytz.timezone('Etc/GMT+5'))
        sql_start = convert_python_time_to_rtc(sql_start_py, pytz.timezone('Etc/GMT+5'))
        sql_end_py = min(series_end, query_end).astimezone(pytz.timezone('Etc/GMT+5'))
        sql_end = convert_python_time_to_rtc(sql_end_py, pytz.timezone('Etc/GMT+5'))

    query_text = "SELECT DISTINCT " + dt_col + ", " + column + " as data_value " \
                 + "FROM " + table \
                 + " WHERE " + column + " IS NOT NULL " \
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

    if len(values_table) > 0:
        if table in ["davis", "CRDavis"]:
            values_table['timestamp'] = values_table[dt_col]
            values_table.set_index(['timestamp'], inplace=True)
            values_table.drop(dt_col, axis=1, inplace=True)
            # NOTE:  The data going into Aquarius MUST already be in the same timezone as that series is in Aquarius
            # TODO: Fix timezones
            values_table.index = values_table.index.tz_localize('UTC')
            if table == "davis":
                values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+5'))
            if table == "CRDavis":
                values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+6'))
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp'] = np.vectorize(convert_rtc_time_to_python)(values_table[dt_col],
                                                                                 pytz.timezone('Etc/GMT+5'))
            values_table.set_index(['timestamp'], inplace=True)
            values_table.drop(dt_col, axis=1, inplace=True)
            values_table.index = values_table.index.tz_convert(pytz.timezone('Etc/GMT+5'))

        if debug:
            print "The first and last rows to append:", values_table.head(1), values_table.tail(1)

    return values_table
