# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This uses command line arguments to decide what to append
"""

import datetime
import time
import pytz
import os
import sys
import argparse
import numpy as np
import Aquarius.aq_utils as aq_utils
import DreamHost.dh_utils as dh_utils

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# %%
# Set up initial parameters - these are rewritten when run from the command prompt.
# Sets number of hours in the past to append.  Use None for all time
past_hours_to_append = None
append_start = None  # Sets start time for the append **in EST**, use None for all time
# append_end = None  # Sets end time for the append **in EST**, use None for all time
# Sets start time for the append **in EST**, use None for all time
# append_start = "2019-09-01 00:00:00"
# Sets end time for the append **in EST**, use None for all time
# append_end = "2016-06-18 15:00:00"
append_end = None
table = None  # Selects a single table to append from, often a logger number, use None for all loggers
column = None  # Selects a single column to append from, often a variable code, use None for all columns


# %%
# Set up a parser for command line options
parser = argparse.ArgumentParser(
    description='This script appends data from Dreamhost to Aquarius.')
parser.add_argument('--debug', action='store_true',
                    help='Turn debugging on')
parser.add_argument('--nolog', action='store_false',
                    help='Turn logging off')
parser.add_argument('--hours', action='store', type=int, default=None,
                    help='Sets number of hours in the past to append')
parser.add_argument('--table', action='store', default=None,
                    help='Selects a single table to append from, often a logger number')
parser.add_argument('--col', action='store', default=None,
                    help='Selects a single column to append from, often a variable code')
parser.add_argument('--start', action='store', default=None,
                    help='Selects a single column to append from, often a variable code')
parser.add_argument('--end', action='store', default=None,
                    help='Selects a single column to append from, often a variable code')

# %%
# Read the command line options, if run from the command line
if sys.stdin.isatty():
    debug = parser.parse_args().debug
    Log_to_file = parser.parse_args().nolog
    past_hours_to_append = parser.parse_args().hours
    append_start = parser.parse_args().start
    append_end = parser.parse_args().end
    table = parser.parse_args().table
    column = parser.parse_args().col
else:
    debug = True
    Log_to_file = True

# %%
# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')


# %%
def start_log():
    # Find the date/time the script was started:
    start_log_dt_utc = datetime.datetime.now(pytz.utc)
    start_log_dt_loc = start_log_dt_utc.astimezone(eastern_local_time)

    # Get the path and directory of this script:
    script_name_with_path = os.path.realpath(__file__)
    script_directory = os.path.dirname(os.path.realpath(__file__))

    if debug:
        print("Now running script: {}".format(script_name_with_path))
        print("Script started at {}".format(start_log_dt_loc))

    # Open file for logging
    if Log_to_file:
        logfile = "{0}\\Aquarius\\AppendLogs\\AppendLog_{1}.txt".format(script_directory,
                                                                        start_log_dt_loc.strftime("%Y%m%d"))
        if debug:
            print("Log being written to: {}".format(logfile))
        open_log_file = open(logfile, "a+")

        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("Script: {} \n".format(script_name_with_path))
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n")
        open_log_file.write(
            "Script started at {} \n \n".format(start_log_dt_loc))
    else:
        open_log_file = ""

    return open_log_file, start_log_dt_utc


# %%
def end_log(open_log_file, start_log_dt_utc):
    # Find the date/time the script finished:
    end_datetime_utc = datetime.datetime.now(pytz.utc)
    end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
    runtime = end_datetime_utc - start_log_dt_utc

    # Close out the text file
    if debug:
        print("Script completed at {}".format(end_datetime_loc))
        print("Total time for script: {}".format(runtime))
    if Log_to_file:
        open_log_file.write("\n")
        open_log_file.write("Script completed at %s \n" % end_datetime_loc)
        open_log_file.write("Total time for script: %s \n" % runtime)
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n \n")
        open_log_file.close()


# %%
# Open the log
text_file, start_datetime_utc = start_log()


# %%
# Set the time cutoff for recent series
# Need to deal with times that are timezone aware/unaware - the MySQL database has no 'aware' timezones
if append_start is None:
    append_start_dt = None
else:
    append_start_dt_naive = datetime.datetime.strptime(
        append_start, "%Y-%m-%d %H:%M:%S")
    append_start_dt = append_start_dt_naive.replace(
        tzinfo=eastern_standard_time)

if append_end is None:
    append_end_dt = None
else:
    append_end_dt_naive = datetime.datetime.strptime(
        append_end, "%Y-%m-%d %H:%M:%S")
    append_end_dt = append_end_dt_naive.replace(tzinfo=eastern_standard_time)

if append_start is None and append_end is None and past_hours_to_append is not None:
    append_end_dt = None
    append_start_utc = start_datetime_utc - \
        datetime.timedelta(hours=past_hours_to_append)
    append_start_dt = append_start_utc.astimezone(eastern_standard_time)


# %%
# Get data for all series that are available
AqSeries, AqData = dh_utils.get_dreamhost_data(required_column='AQTimeSeriesID',
                                               query_start=append_start_dt, query_end=append_end_dt,
                                               data_table_name=table, data_column_name=column, debug=debug)
AqSeries = AqSeries.sort_values(by=['TableName', 'DateTimeSeriesStart', 'TableColumnName'])
AqData = AqData.sort_values(by=['TableName', 'DateTimeSeriesStart', 'TableColumnName', 'timestamp'])

if Log_to_file:
    text_file.write("{} series found with corresponding time series in Aquarius \n \n".format(
        len(AqSeries.index)))

if len(AqData.index) > 0:
    if Log_to_file:
        text_file.write(
            "Series, Table, Column, NumericIdentifier, TextIdentifier, NumPointsAppended, AppendToken  \n")

    # Get the corresponding Aquarius series time zones for each time series
    get_aq_timezone = np.vectorize(aq_utils.get_aquarius_timezone)
    AqSeries['AQTimeZone'] = get_aq_timezone(
        AqSeries['AQTimeSeriesID'], AqSeries['AQLocationID'])
    AqSeries2 = AqSeries.loc[:, ['SeriesID', 'AQTimeZone']]

    # Merge the Aquarius timezone with the data
    AqData = AqData.merge(AqSeries2, how='left', on='SeriesID')
    # Localize data to the Aquarius timezone
    AqData['AQLocalizedTimeStamp'] = AqData.apply(
        lambda row1: row1.timestamp.astimezone(row1.AQTimeZone), axis=1)

    i = 1
    for name, group in AqData.groupby('AQTimeSeriesID'):
        append_bytes = aq_utils.create_appendable_csv(group)
        AppendResult = aq_utils.aq_timeseries_append(
            name, append_bytes, debug=debug)
        # TODO: stop execution of further requests after an error.
        if Log_to_file:
            text_file.write("{}, {}, {}, {}, {}, {}, {} \n"
                            .format(i, group['TableName'].iloc[0], group['TableColumnName'].iloc[0],
                                    name, AppendResult.TsIdentifier,
                                    AppendResult.NumPointsAppended, AppendResult.AppendToken))
        time.sleep(1)
        i += 1


# Close out the text file
end_log(text_file, start_datetime_utc)
