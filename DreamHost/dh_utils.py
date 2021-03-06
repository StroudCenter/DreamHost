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
    unix_time = (datetime_aware -
                 timezone.localize(datetime.datetime(1970, 1, 1))).total_seconds()
    sec_from_rtc_epoch = unix_time - 946684800
    return sec_from_rtc_epoch


def get_dreamhost_data(required_column="SeriesID", query_start=None, query_end=None,
                       data_table_name=None, data_column_name=None, debug=False):
    """
    Gets all the data and series from a dreamhost series that has a required column
    :arguments:
    required_column = A string column name which must not be blank in the query
    query_start = A datetime string which data must be newer than, defaults to none.
    query_end = A datetime string which data must be older than, defaults to none.
    dataTableName = A string table name, if data from only one is desired.
    dataColumnName = A string column name, if data from only one is desired
    :return:
    Returns a list of series.
    """

    # Get the actual data for each series
    series_table = get_dreamhost_series_table(required_column=required_column,
                                              series_query_start=query_start, series_query_end=query_end,
                                              data_table_name=data_table_name, data_column_name=data_column_name,
                                              debug=debug)

    # Create a new table to append data to
    series_table_with_data = series_table

    for (idx, row) in series_table.iterrows():
        data_dt = get_data_from_dreamhost_table(table=row.TableName, column=row.TableColumnName,
                                                data_query_start=row.DateTimeQueryStart,
                                                data_query_end=row.DateTimeQueryEnd,
                                                debug=debug)
        series_table.loc[idx, 'NumberDataValues'] = len(data_dt.index)
        if len(data_dt.index) > 0:
            data_dt['SeriesID'] = row.SeriesID

            series_table_with_data = series_table_with_data.merge(
                data_dt, how='outer', on='SeriesID')
            if 'data_value_x' in series_table_with_data:
                series_table_with_data['data_value'] = \
                    series_table_with_data['data_value_x'].fillna(
                        series_table_with_data['data_value_y'])
                series_table_with_data.drop(
                    'data_value_x', axis=1, inplace=True)
                series_table_with_data.drop(
                    'data_value_y', axis=1, inplace=True)

                series_table_with_data['timestamp'] = \
                    series_table_with_data['timestamp_x'].fillna(
                        series_table_with_data['timestamp_y'])
                series_table_with_data.drop(
                    'timestamp_x', axis=1, inplace=True)
                series_table_with_data.drop(
                    'timestamp_y', axis=1, inplace=True)

                series_table_with_data['server_offset'] = \
                    series_table_with_data['server_offset_x'].fillna(
                        series_table_with_data['server_offset_y'])
                series_table_with_data.drop(
                    'server_offset_x', axis=1, inplace=True)
                series_table_with_data.drop(
                    'server_offset_y', axis=1, inplace=True)

                series_table_with_data['time_correction'] = \
                    series_table_with_data['time_correction_x'].fillna(
                        series_table_with_data['time_correction_y'])
                series_table_with_data.drop(
                    'time_correction_x', axis=1, inplace=True)
                series_table_with_data.drop(
                    'time_correction_y', axis=1, inplace=True)

    # Check number of values returned
    series_table_with_data["NumberDataValues"] = \
        series_table_with_data.groupby(["SeriesID"])[
        'data_value'].transform('count')
    # Remove rows where no values were returned
    series_table_with_data = \
        series_table_with_data.drop(
            series_table_with_data[series_table_with_data.NumberDataValues == 0].index)
    series_table = \
        series_table.drop(
            series_table[series_table.NumberDataValues == 0].index)

    # Sort by date
    series_table_with_data.sort_values(
        by=['TableName', 'TableColumnName', 'timestamp'], inplace=True)
    series_table.sort_values(
        by=['TableName', 'TableColumnName', 'DateTimeSeriesStart'], inplace=True)

    # Reset indices after sorting
    series_table_with_data = series_table_with_data.reset_index(drop=True)
    series_table = series_table.reset_index(drop=True)

    return series_table, series_table_with_data


def get_dreamhost_series_table(required_column="SeriesID", series_query_start=None, series_query_end=None,
                               data_table_name=None, data_column_name=None, debug=False):
    """
    Gets a list of all the series to append data to
    :arguments:
    required_column = A string column name which must not be blank in the query
    series_query_start = A datetime string which data must be newer than, defaults to none.
    series_query_end = A datetime string which data must be older than, defaults to none.
    dataTableName = A string table name, if data from only one is desired.
    dataColumnName = A string column name, if data from only one is desired
    :return:
    Returns a list of series.
    """

    # Set up an min and max time for when those values are not given
    if series_query_start is None:
        str1 = ""
        series_query_start = datetime.datetime(
            2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
    else:
        str1 =\
            " AND ( DateTimeSeriesEnd is NULL OR " + \
            "DateTimeSeriesEnd >= '" + \
            str(series_query_start.strftime("%Y-%m-%d %H:%M:%S")) + "' )"
        # + \
        # " AND ( DateTimeSeriesStart is NULL OR " + \
        # "DateTimeSeriesStart >= '" + str(series_query_start.strftime("%Y-%m-%d %H:%M:%S")) + "' )"

    if series_query_end is None:
        series_query_end = datetime.datetime.now(
            pytz.timezone('Etc/GMT+5')) + datetime.timedelta(days=1)
        str2 = ""
    else:
        str2 =\
            " AND ( DateTimeSeriesStart is NULL OR " + \
            "DateTimeSeriesStart <= '" + \
            str(series_query_end.strftime("%Y-%m-%d %H:%M:%S")) + "' )"
        # + \
        # " AND ( DateTimeSeriesEnd is NULL OR " + \
        # "DateTimeSeriesEnd >= '" + str(series_query_end.strftime("%Y-%m-%d %H:%M:%S")) + "' )"

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
        print("Timeseries selected using the query:")
        print(query_text)

    # Set up connection to the DreamHost MySQL database
    conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                           user=dh_db_user, passwd=dh_db_pass)
    # cur = conn.cursor()

    # Create a pandas data frame from the query
    series_table = pd.read_sql(query_text, conn)

    site_query = \
        "SELECT DISTINCT SiteID, SiteCode, AQLocationID, EnviroDIYToken, SamplingFeatureGUID" \
        " FROM Sites_for_midStream" \
        " WHERE AQLocationID is not NULL OR EnviroDIYToken is not NULL"
    sites = pd.read_sql(site_query, conn)
    sites['AQLocationID'] = sites['AQLocationID'].fillna(0).astype('int64')

    series_table = series_table.merge(sites, on="SiteID")

    if debug:
        print("which returns {} series".format(len(series_table.index)))

    # Fill in any missing time zones with '-5'
    series_table['SeriesTimeZone'].fillna(value=-5, inplace=True)

    # create a series/column with the string timezone name
    series_table['utc_offset_string'] = 'Etc/GMT+' + \
        (-1 * series_table['SeriesTimeZone']).map('{:.0f}'.format)
    # NOTE:  In the Olson TZ Database used by pytz "Etc/GMT+5" is the name of the timezone at UTC-5.
    # From Wikipedia:  In order to conform with the POSIX style, those zone names beginning with "Etc/GMT" have their
    # sign reversed from the standard ISO 8601 convention. In the "Etc" area, zones west of GMT have a positive sign
    # and those east have a negative sign in their name (e.g "Etc/GMT-14" is 14 hours ahead of GMT.)
    series_table['utc_offset_string'] = series_table['utc_offset_string'].str.replace(
        "+-", "-", regex=False)

    # Fix the types of the date/time columns
    series_table['DateTimeSeriesStart'] = pd.to_datetime(
        series_table['DateTimeSeriesStart'], errors='coerce')
    series_table['DateTimeSeriesEnd'] = pd.to_datetime(
        series_table['DateTimeSeriesEnd'], errors='coerce')

    # Set up an min and max time for when those values are NULL in dreamhost
    # series_table['DateTimeSeriesStart'].fillna(np.datetime64('2000-01-01T00:00:00'))
    # series_table['DateTimeSeriesEnd'].fillna(np.datetime64('now') + np.timedelta64(1, 'D'))

    # Localize the datetime columns based on the timezone name
    series_table['DateTimeSeriesStart'] = \
        series_table.apply(lambda row1: pytz.timezone(row1.utc_offset_string).localize(row1.DateTimeSeriesStart),
                           axis=1)
    series_table['DateTimeSeriesEnd'] = \
        series_table.apply(lambda row1: pytz.timezone(row1.utc_offset_string).localize(row1.DateTimeSeriesEnd),
                           axis=1)

    # Verify the actual date and time to pick from the dream host tables
    series_table['DateTimeQueryStart'] = series_table.apply(lambda row1:
                                                            max(series_query_start, row1.DateTimeSeriesStart), axis=1)
    series_table['DateTimeQueryEnd'] = series_table.apply(lambda row1:
                                                          min(series_query_end, row1.DateTimeSeriesEnd), axis=1)

    return series_table


def get_data_from_dreamhost_table(table, column, data_query_start=None, data_query_end=None, debug=False):
    """
    Returns a pandas data frame with the timestamp and data value from a given table and column.
    :param table: A string which is the same of the SQL table of interest
    :param column: A string which is the name of the column of interest
    :param data_query_start: The first date/time for data - All date times should be timezone AWARE
    :param data_query_end: The last date/time for data
    :param debug: A boolean for whether extra print commands apply
    :return: A pandas data frame with the timestamp and data value from a given table and column.
    """

    # Set up an min and max time for when those values are not given
    if data_query_start is None:
        data_query_start = datetime.datetime(
            2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
    if data_query_end is None:
        data_query_end = datetime.datetime.now(
            pytz.timezone('Etc/GMT+5')) + datetime.timedelta(days=1)

    start_tz = data_query_start.tzinfo
    # end_tz = data_query_end.tzinfo

    # if table in ["davis", "CRDavis"]:
    #     # The meteobridges streaming this data stream a column of time in UTC
    #     dt_col = "mbutcdatetime"
    #     dt_server_col = "servertime"
    #     sql_start = data_query_start.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    #     sql_end = data_query_end.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    # elif table in ["SL157", "SL111", "SL112"]:  # This logger's timestamp is a year off..
    #     dt_col = "Loggertime"
    #     dt_server_col = "Date"
    #     if table == "SL157":  # A year off throughout
    #         sql_start = convert_python_time_to_rtc(data_query_start, start_tz) - 31536000
    #         sql_end = convert_python_time_to_rtc(data_query_end, end_tz) - 31536000
    #     if table == "SL111":  # A year off only for portions
    #         if data_query_start == datetime.datetime(2017, 6, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5')):  # unset clock...
    #             sql_start = 0
    #         elif data_query_start > datetime.datetime(2018, 2, 9, 12, 9, 0, tzinfo=pytz.timezone('Etc/GMT+5')):
    #             sql_start = convert_python_time_to_rtc(data_query_start, start_tz) - 31536000
    #         else:
    #             sql_start = convert_python_time_to_rtc(data_query_start, start_tz)
    #         if data_query_end > datetime.datetime(2018, 2, 9, 12, 9, 0, tzinfo=pytz.timezone('Etc/GMT+5')):
    #             sql_end = convert_python_time_to_rtc(data_query_end, end_tz) - 31536000
    #         else:
    #             sql_end = convert_python_time_to_rtc(data_query_end, end_tz)
    #     if table == "SL112":  # A year off only for portions
    #         if data_query_start > datetime.datetime(2018, 4, 19, 14, 59, 0, tzinfo=pytz.timezone('Etc/GMT+5')):
    #             sql_start = convert_python_time_to_rtc(data_query_start, start_tz) - 31536000
    #         else:
    #             sql_start = convert_python_time_to_rtc(data_query_start, start_tz)
    #         if data_query_end > datetime.datetime(2018, 4, 19, 14, 59, 0, tzinfo=pytz.timezone('Etc/GMT+5')):
    #             sql_end = convert_python_time_to_rtc(data_query_end, end_tz) - 31536000
    #         else:
    #             sql_end = convert_python_time_to_rtc(data_query_end, end_tz)
    # else:
    #     dt_col = "Loggertime"
    #     dt_server_col = "Date"
    #     sql_start = convert_python_time_to_rtc(data_query_start, start_tz)
    #     sql_end = convert_python_time_to_rtc(data_query_end, end_tz)

    if table in ["davis", "CRDavis"]:
        # The meteobridges streaming this data stream a column of time in UTC
        dt_col = "mbutcdatetime"
        dt_server_col = "servertime"
        sql_start = data_query_start.astimezone(
            'US/Pacific').strftime("%Y-%m-%d %H:%M:%S")
        sql_end = data_query_end.astimezone(
            'US/Pacific').strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt_col = "Loggertime"
        dt_server_col = "Date"
        sql_start = data_query_start.astimezone(
            pytz.timezone('Etc/GMT+5')).strftime("%Y-%m-%d %H:%M:%S")
        sql_end = data_query_end.astimezone(pytz.timezone(
            'Etc/GMT+5')).strftime("%Y-%m-%d %H:%M:%S")

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    query_text = "SELECT DISTINCT " + dt_col + ", " + dt_server_col + ", " + column + " as data_value" \
                 + " FROM " + table  \
                 + " WHERE " + dt_server_col + " IS NOT NULL" \
                 + " AND " + dt_server_col + " >= '" + str(sql_start) + "'" \
                 + " AND " + dt_server_col + " <= '" + str(sql_end) + "'" \
                 + " ORDER BY " + dt_server_col + ", " + dt_col \
                 + " ;"

    if debug:
        print("Data selected using the query:")
        print("   " + query_text)
    t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    if table in ["SL035", "SL036", "SL037"]:
        conn = pymysql.connect(
            host=dh_db_host, db=dh_db_name_cib, user=dh_db_user, passwd=dh_db_pass)
    else:
        conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                               user=dh_db_user, passwd=dh_db_pass)

    try:
        values_table = pd.read_sql(query_text, conn)
    except pd.io.sql.DatabaseError as e:
        if debug:
            print("   ERROR: ", e)
        conn.close()  # close the database connection
        return pd.DataFrame(columns=['timestamp', 'data_value'])
    else:
        conn.close()  # close the database connection

    if debug:
        t2 = datetime.datetime.now()
        print("   which returns {} values in {}".format(
            len(values_table), (t2 - t1)))

    # Create a new column with a proper uniform python datetime data type
    # remove old datetime column
    # values_table = pd.read_sql(query_text, conn)
    if values_table[dt_col].count() > 0:
        if table in ["davis", "CRDavis"]:
            values_table['timestamp_raw'] = \
                values_table.apply(lambda row1: pytz.utc.localize(
                    row1.mbutcdatetime).astimezone(start_tz), axis=1)
            values_table['server_timestamp'] = values_table[dt_server_col].dt.tz_localize(
                tz="US/Pacific", ambiguous=True)
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp_raw'] = np.vectorize(
                convert_rtc_time_to_python)(values_table[dt_col], start_tz)
            values_table['server_timestamp'] = values_table[dt_server_col].dt.tz_localize(
                tz="Etc/GMT+5")

        # Fix timestamps from badly programmed loggers
        # if table in ["SL157"]:
        #     bad_program_dt = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
        # elif table in ["SL111"]:
        #     bad_program_dt = datetime.datetime(2018, 2, 9, 12, 10, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
        # elif table in ["SL112"]:  # This logger's timestamp is a year off..
        #     bad_program_dt = datetime.datetime(2018, 4, 19, 15, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
        # else:
        #     bad_program_dt = data_query_end

        # estimate what we should be correcting by
        values_table['server_offset'] = (values_table['server_timestamp'].dt.tz_convert(tz="Etc/GMT+5") -
                                         values_table['timestamp_raw'].dt.tz_convert(tz="Etc/GMT+5"))
        values_table['server_offset_round'] = values_table['server_offset'].dt.floor(
            freq='5min')

        # don't correct if the needed correction would be less than 5 minutes
        values_table['mask'] = abs(
            values_table['server_offset']) > pd.Timedelta(minutes=5)
        values_table['time_correction'] = values_table['server_offset_round'].where(
            values_table['mask'])
        values_table['time_correction'].fillna(
            pd.Timedelta(seconds=0), inplace=True)

        # Actually do the correction
        values_table['timestamp'] = values_table['timestamp_raw'] + \
            values_table['time_correction']
        values_table.sort_values(by=['timestamp'], inplace=True)
        values_table = values_table.reset_index(drop=True)

        # Drop extra columns
        values_table.drop(dt_col, axis=1, inplace=True)
        values_table.drop(dt_server_col, axis=1, inplace=True)
        values_table.drop('server_timestamp', axis=1, inplace=True)
        values_table.drop('timestamp_raw', axis=1, inplace=True)
        values_table.drop('server_offset_round', axis=1, inplace=True)
        values_table.drop('mask', axis=1, inplace=True)

        # Drop values that came in that are out of the date range after correcting the timestamp
        values_table.drop(
            values_table[values_table.timestamp < data_query_start].index, inplace=True)
        values_table.drop(
            values_table[values_table.timestamp > data_query_end].index, inplace=True)

        # if debug:
        #     print "The first and last rows from DreamHost:\r\n", values_table.head(2), "\r\n", values_table.tail(2)

    return values_table


def get_min_max_from_dreamhost_table(table, column, min=True, debug=True):
    """
    Returns a pandas data frame with the timestamp and data value from a given table and column.
    :param table: A string which is the same of the SQL table of interest
    :param column: A string which is the name of the column of interest
    :param debug: A boolean for whether extra print commands apply
    :return: A pandas data frame with the timestamp and data value from a given table and column.
    """

    # Set up a sanity check min/max
    data_query_start = datetime.datetime(
        2000, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('Etc/GMT+5'))
    data_query_end = datetime.datetime.now(
        pytz.timezone('Etc/GMT+5')) + datetime.timedelta(days=1)

    start_tz = data_query_start.tzinfo
    end_tz = data_query_end.tzinfo

    if table in ["davis", "CRDavis"]:
        # The meteobridges streaming this data stream a column of time in UTC
        dt_col = "mbutcdatetime"
        sql_start = data_query_start.astimezone(
            pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql_end = data_query_end.astimezone(
            pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        dt_col = "Loggertime"
        sql_start = convert_python_time_to_rtc(data_query_start, start_tz)
        sql_end = convert_python_time_to_rtc(data_query_end, end_tz)

    if min:
        select_col = "MIN(" + dt_col + ") as " + dt_col
    else:
        select_col = "MAX(" + dt_col + ") as " + dt_col

    # Creating the query text here because the character masking works oddly
    # in the cur.execute function.
    query_text = "SELECT DISTINCT " + select_col \
                 + " FROM " + table  \
                 + " WHERE " + column + " IS NOT NULL" \
                 + " AND " + dt_col + " IS NOT NULL" \
                 + " AND " + dt_col + " >= '" + str(sql_start) + "'" \
                 + " AND " + dt_col + " <= '" + str(sql_end) + "'" \
                 + " ;"

    if debug:
        print("Data selected using the query:")
        print("   " + query_text)
    t1 = datetime.datetime.now()

    # Set up connection to the DreamHost MySQL database
    if table in ["SL035", "SL036", "SL037"]:
        conn = pymysql.connect(
            host=dh_db_host, db=dh_db_name_cib, user=dh_db_user, passwd=dh_db_pass)
    else:
        conn = pymysql.connect(host=dh_db_host, db=dh_db_name,
                               user=dh_db_user, passwd=dh_db_pass)

    try:
        values_table = pd.read_sql(query_text, conn)
    except pd.io.sql.DatabaseError as e:
        if debug:
            print("   ERROR: ", e)
        conn.close()  # close the database connection
        return pd.DataFrame(columns=['timestamp', 'data_value'])
    else:
        conn.close()  # close the database connection

    if debug:
        t2 = datetime.datetime.now()
        print("   which returns {} values in {}".format(
            len(values_table), (t2 - t1)))

    # Create a new column with a proper uniform python datetime data type
    # remove old datetime column
    if values_table[dt_col].count() > 0:
        if table in ["davis", "CRDavis"]:
            values_table['timestamp'] = \
                values_table.apply(lambda row1: pytz.utc.localize(
                    row1.mbutcdatetime).astimezone(start_tz), axis=1)
            values_table.drop(dt_col, axis=1, inplace=True)
        elif table in ["SL157"]:  # This logger's timestamp is a year off..
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp'] = np.vectorize(
                convert_rtc_time_to_python)(values_table[dt_col], start_tz)
            values_table['timestamp'] = values_table['timestamp'].add(
                pd.Timedelta(days=365))
            values_table.drop(dt_col, axis=1, inplace=True)
        else:
            # Need to convert arduino logger time into unix time (add 946684800)
            values_table['timestamp'] = np.vectorize(
                convert_rtc_time_to_python)(values_table[dt_col], start_tz)
            values_table.drop(dt_col, axis=1, inplace=True)

        # if debug:
        #     print "The first and last rows from DreamHost:\r\n", values_table.head(2), "\r\n", values_table.tail(2)

    if min:
        values_table.rename(
            columns={'timestamp': 'timestamp_min'}, inplace=True)
    else:
        values_table.rename(
            columns={'timestamp': 'timestamp_max'}, inplace=True)

    return values_table
