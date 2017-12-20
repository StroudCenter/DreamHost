# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 5/16/2016 at 6:14 PM


"""

import suds
import time
import datetime
import pytz
import base64
import sys
import socket

# Bring in all of the database connection information.
from dbinfo import aq_acquisition_url, aq_username, aq_password

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'


# Get an authentication token to open the path into the API
def get_aq_auth_token(username, password, debug=False):
    """
    Sets up an authentication token for the soap session
    """
    # Call up the Aquarius Aquisition SOAP API

    try:
        client = suds.client.Client(aq_acquisition_url, timeout=30)
    except Exception, e:
        if debug:
            print "Error Getting Token: %s" % sys.exc_info()[0]
            print '%s' % e
            print "Stopping all program execution"
        sys.exit("Unable to connect to server")
    else:
        try:
            auth_token = client.service.GetAuthToken(username, password)
            cookie = client.options.transport.cookiejar
        except suds.WebFault, e:
            if debug:
                print "Error Getting Token: %s" % sys.exc_info()[0]
                print '%s' % e
                print "Stopping all program execution"
            sys.exit("No Authentication Token")
        else:
            if debug:
                print "Authentication Token: %s" % auth_token
                print "Session Cookie %s" % cookie
            return auth_token, cookie


load_auth_token, load_cookie = get_aq_auth_token(aq_username, aq_password)


def check_aq_connection(cookie=load_cookie):
    # Call up the Aquarius Acquisition SOAP API
    try:
        client = suds.client.Client(aq_acquisition_url, timeout=30)
        client.options.transport.cookiejar = cookie
    except Exception, e:
        is_valid = False
    else:
        try:
            is_valid = client.service.IsConnectionValid()
        except Exception, e:
            is_valid = False
        else:
            e = ""
    return is_valid, e


def get_aquarius_timezone(ts_numeric_id, loc_numeric_id=None, cookie=load_cookie):
    # Call up the Aquarius Acquisition SOAP API
    client = suds.client.Client(aq_acquisition_url, timeout=30)
    client.options.transport.cookiejar = cookie

    if loc_numeric_id is None:
        all_locations = client.service.GetAllLocations().LocationDTO
    else:
        all_locations = []
        locdto = client.service.GetLocation(loc_numeric_id)
        all_locations.append(locdto)
    for location in all_locations:
        utc_offset_float = location.UtcOffset
        utc_offset_string = '{:+3.0f}'.format(utc_offset_float*-1).strip()
        timezone = pytz.timezone('Etc/GMT'+utc_offset_string)
        all_descriptions_array = client.service.GetTimeSeriesListForLocation(location.LocationId)
        try:
            all_descriptions = all_descriptions_array.TimeSeriesDescription
        except AttributeError:
            pass
        else:
            for description in all_descriptions:
                ts_id = description.AqDataID
                if ts_id == ts_numeric_id:
                    return timezone
    return None


def create_appendable_csv(data_table):
    """
    This takes a pandas data frame and converts it to a base64 string ready to read into the
    Aquarius API.
    :param data_table: A python data frame with a date-time index and a "value" column.
        It also, optionally, can have the fields "flag", "grade", "interpolation",
        "approval", and "note".
    :return: A base64 text string.
    """

    if len(data_table) > 0:
        if 'flag' not in data_table:
            data_table.loc[:, 'flag'] = ""
        if 'grade' not in data_table:
            data_table.loc[:, 'grade'] = ""
        if 'interpolation' not in data_table:
            data_table.loc[:, 'interpolation'] = ""
        if 'approval' not in data_table:
            data_table.loc[:, 'approval'] = ""
        if 'note' not in data_table:
            data_table.loc[:, 'note'] = ""

        # Output a CSV
        csvdata = data_table.to_csv(header=False, date_format='%Y-%m-%d %H:%M:%S')

        # Convert the data string into a base64 object
        csvbytes = base64.b64encode(csvdata)

    else:
        csvbytes = ""

    return csvbytes


def aq_timeseries_append(ts_numeric_id, appendbytes, cookie=load_cookie, debug=False):
    """
    Appends data to an aquarius time series given a base64 encoded csv string with the following values:
        datetime(isoformat), value, flag, grade, interpolation, approval, note
    :param ts_numeric_id: The integer primary key of an aquarius time series
    :param appendbytes: Base64 csv string with ISO-datetime, value, flag, grade, interpolation, approval, note
    :param cookie: The cookie wiith the session ID.  Get via the get_aq_auth_token function.
    :param debug: Says whether or not to issue print statements.
    :return: The append result from the SOAP client
    """

    # Call up the Aquarius Acquisition SOAP API
    client = suds.client.Client(aq_acquisition_url, timeout=1500)
    client.options.transport.cookiejar = cookie
    empty_result = client.factory.create('ns0:AppendResult')

    # Actually append to the Aquarius dataset
    t3 = datetime.datetime.now()
    if len(appendbytes) > 0:
        for attempt in range(10):
            try:
                append_result = client.service.AppendTimeSeriesFromBytes2(long(ts_numeric_id),
                                                                          appendbytes,
                                                                          aq_username)
            except suds.WebFault, e:
                if debug:
                    print "      Error: %s" % sys.exc_info()[0]
                    print '      %s' % e
                    t4 = datetime.datetime.now()
                    print "      API execution took %s" % (t4 - t3)
                empty_result.NumPointsAppended = 0
                empty_result.AppendToken = 0
                empty_result.TsIdentifier = '"Error: "' + str(e) + '"'
                append_result = empty_result
                break
            except socket.timeout, e:
                if debug:
                    print "      Error: %s" % sys.exc_info()[0]
                    print '      %s' % e
                    print '      Retrying in 30 seconds'
                time.sleep(30)
            else:
                if debug:
                    print append_result
                    t4 = datetime.datetime.now()
                    print "      API execution took %s" % (t4 - t3)
                    print "SUCCESS!"
                break
        else:
            if debug:
                print "      Error: %s" % sys.exc_info()[0]
                print '      10 retries attempted'
                t4 = datetime.datetime.now()
                print "      API execution took %s" % (t4 - t3)
            empty_result.NumPointsAppended = 0
            empty_result.AppendToken = 0
            empty_result.TsIdentifier = '"Error: "' + sys.exc_info()[0] + '"'
            append_result = empty_result
    else:
        if debug:
            print "      No data appended from this query."
        empty_result.NumPointsAppended = 0
        empty_result.AppendToken = 0
        empty_result.TsIdentifier = ""
        append_result = empty_result

    return append_result
