# -*- coding: utf-8 -*-

"""
Created on Wed Nov 05 13:58:15 2014

@author: Sara Geleskie Damiano

This script moves all data from series tagged with an Aquarius dataset primary key
from a DreamHost database to Stroud's Aquarius server.
This uses command line arguments to decide what to append
"""

import datetime
import pytz
import os
import sys
import argparse
import DreamHost.dh_utils as dh_utils
import requests

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'

# Set up initial parameters - these are rewritten when run from the command prompt.
past_hours_to_append = None  # Sets number of hours in the past to append.  Use None for all time
# append_start = None  # Sets start time for the append **in EST**, use None for all time
append_end = None  # Sets end time for the append **in EST**, use None for all time
append_start = "2018-01-22 17:00:00"  # Sets start time for the append **in EST**, use None for all time
# append_end = "2017-05-01 00:00:00"  # Sets end time for the append **in EST**, use None for all time
table = None  # Selects a single table to append from, often a logger number, use None for all loggers
column = None  # Selects a single column to append from, often a variable code, use None for all columns


# Set up a parser for command line options
parser = argparse.ArgumentParser(description='This script appends data from Dreamhost to Aquarius.')
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

# Read the command line options, if run from the command line
if sys.stdin.isatty():
    debug = parser.parse_args().debug
    Log_to_file = parser.parse_args().nolog
    past_hours_to_append = parser.parse_args().hours
    append_start = None
    append_end = None
    table = parser.parse_args().table
    column = parser.parse_args().col
else:
    debug = True
    Log_to_file = True

# Deal with timezones...
eastern_standard_time = pytz.timezone('Etc/GMT+5')
eastern_local_time = pytz.timezone('US/Eastern')


def start_log():
    # Find the date/time the script was started:
    start_log_dt_utc = datetime.datetime.now(pytz.utc)
    start_log_dt_loc = start_log_dt_utc.astimezone(eastern_local_time)

    # Get the path and directory of this script:
    script_name_with_path = os.path.realpath(__file__)
    script_directory = os.path.dirname(os.path.realpath(__file__))

    if debug:
        print "Now running script: %s" % script_name_with_path
        print "Script started at %s" % start_log_dt_loc

    # Open file for logging
    if Log_to_file:
        logfile = script_directory + "\EnviroDIY\AppendLogs\AppendLog_" + start_log_dt_loc.strftime("%Y%m%d") + ".txt"
        if debug:
            print "Log being written to: %s" % logfile
        open_log_file = open(logfile, "a+")

        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("Script: %s \n" % script_name_with_path)
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n")
        open_log_file.write("Script started at %s \n \n" % start_log_dt_loc)
    else:
        open_log_file = ""

    return open_log_file, start_log_dt_utc


def end_log(open_log_file, start_log_dt_utc):
    # Find the date/time the script finished:
    end_datetime_utc = datetime.datetime.now(pytz.utc)
    end_datetime_loc = end_datetime_utc.astimezone(eastern_local_time)
    runtime = end_datetime_utc - start_log_dt_utc

    # Close out the text file
    if debug:
        print "Script completed at %s" % end_datetime_loc
        print "Total time for script: %s" % runtime
    if Log_to_file:
        open_log_file.write("\n")
        open_log_file.write("Script completed at %s \n" % end_datetime_loc)
        open_log_file.write("Total time for script: %s \n" % runtime)
        open_log_file.write(
            "*******************************************************************************************************\n")
        open_log_file.write("\n \n")
        open_log_file.close()


# Open the log
text_file, start_datetime_utc = start_log()


# Set the time cutoff for recent series
# Need to deal with times that are timezone aware/unaware - the MySQL database has no 'aware' timezones
if append_start is None:
    append_start_dt = None
else:
    append_start_dt_naive = datetime.datetime.strptime(append_start, "%Y-%m-%d %H:%M:%S")
    append_start_dt = append_start_dt_naive.replace(tzinfo=eastern_standard_time)

if append_end is None:
    append_end_dt = None
else:
    append_end_dt_naive = datetime.datetime.strptime(append_end, "%Y-%m-%d %H:%M:%S")
    append_end_dt = append_end_dt_naive.replace(tzinfo=eastern_standard_time)

if append_start is None and append_end is None and past_hours_to_append is not None:
    append_end_dt = None
    append_start_utc = start_datetime_utc - datetime.timedelta(hours=past_hours_to_append)
    append_start_dt = append_start_utc.astimezone(eastern_standard_time)


# Get data for all series that are available
DIYSeries, DIYData = dh_utils.get_dreamhost_data(required_column='TimeSeriesGUID',
                                                 query_start=append_start_dt, query_end=append_end_dt,
                                                 data_table_name=table, data_column_name=column, debug=debug)

if Log_to_file:
    text_file.write("%s series found with corresponding time series on the EnviroDIY data portal \n \n"
                    % (len(DIYSeries.index)))

if len(DIYData.index) > 0:
    if Log_to_file:
        text_file.write("Table, Timestamp, HTTPAppendResult  \n")

    for name, group in DIYData.groupby(['EnviroDIYToken', 'SamplingFeatureGUID', 'timestamp']):
        json_string = '{\r\n"sampling_feature": "'
        json_string += group.iloc[0].SamplingFeatureGUID
        json_string += '",\r\n"timestamp": "'
        json_string += group.iloc[0].timestamp.isoformat()
        json_string += '"'
        for idx, row in group.iterrows():
            json_string += ',\r\n    "'
            json_string += row.TimeSeriesGUID
            json_string += '": '
            json_string += str(row.data_value)
        json_string += '\r\n}'

        response = requests.post(url='http://data.envirodiy.org/api/data-stream/',
                                 headers={"TOKEN": group.iloc[0].EnviroDIYToken, 'Content-Type': 'application/json'},
                                 data=json_string)
        if debug:
            print group.iloc[0].TableName, "-", group.iloc[0].timestamp.isoformat(), "-", response.status_code
            # print "    ", response.request.method, response.request.path_url
            # print "    ", response.request.headers
            # print "    ", response.request.body
            # print response.request.body
            # print "    ", "Response status code: %s" % response.status_code
            if response.status_code > 205:
                print "    ", response.text

        if Log_to_file:
            text_file.write("%s, %s, %s  \n" %
                            (group.iloc[0].TableName, group.iloc[0].timestamp, response.status_code))

# Close out the text file
end_log(text_file, start_datetime_utc)
