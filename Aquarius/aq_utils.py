# -*- coding: utf-8 -*-


"""
Created by Sara Geleskie Damiano on 5/16/2016 at 6:14 PM


"""

import time
import datetime
import pytz
import base64
import sys
import socket
from zeep.transports import Transport
from zeep import Client
import pandas as pd

# Bring in all of the database connection information.
from Aquarius.aq_dbinfo import aq_acq_1page_url, aq_username, aq_password

__author__ = 'Sara Geleskie Damiano'
__contact__ = 'sdamiano@stroudcenter.org'


# Get an authentication token to open the path into the API
def get_aq_auth_token(username=aq_username, password=aq_password, debug=False):
    """
    Sets up an authentication token for the soap session
    """
    # Call up the Aquarius Acquisition SOAP API

    try:
        token_transport = Transport(timeout=30)
        aq_token_client = Client(aq_acq_1page_url, transport=token_transport)
    except Exception as e:
        print("Error Creating Client: {}".format(sys.exc_info()[0]))
        print('{}'.format(e))
        print("Stopping all program execution")
        sys.exit("Unable to connect to server")
    else:
        try:
            auth_token = aq_token_client.service.GetAuthToken(
                username, password)
            # cookie = aq_token_client.options.transport.cookiejar
        except suds.WebFault as e:
            if debug:
                print("Error Getting Acquisition Token: {}".format(
                    sys.exc_info()[0]))
                print('{}'.format(e))
                print("Stopping all program execution")
            sys.exit("No Acquisition Authentication Token")
        else:
            if debug:
                print("Acquisition Authentication Token: {}".format(auth_token))
                # print("Acquisition Session Cookie {}".format(cookie))
            return auth_token


# The client, with a transport to increase the timeout
transport = Transport(timeout=600)
aq_client = Client(aq_acq_1page_url, transport=transport)
# Run the get-authentication function to get a default cookie
module_token = get_aq_auth_token(aq_username, aq_password)


def check_aq_connection(token=module_token, debug=False):
    start_check = datetime.datetime.now()
    try:
        is_valid = aq_client.service.IsConnectionValid(
            _soapheaders={"AQAuthToken": token})
    except Exception as e:
        is_valid = False
        if debug:
            print("No valid connection to the Aquarius acquisition endpoint.")
            print("Server returned error: {}".format(e))
    if is_valid:
        end_check = datetime.datetime.now()
        if debug:
            print("Valid connection returned after {} seconds".format(
                end_check - start_check))
        # Keep the connection alive!
        try:
            if debug:
                print("Requesting that token {} be kept alive".format(token))
            aq_client.service.KeepConnectionAlive(
                _soapheaders={"AQAuthToken": token})
        except Exception as e:
            print("Unable to request keep-alive: {}".format(e))
    return is_valid


def check_and_revalidate_connection(token=module_token, debug=False):
    global module_token
    if debug:
        print("Checking for valid connection to the Aquarius acquisition endpoint; token: {}.".format(
            module_token))
    if not check_aq_connection(token, debug):
        module_token = get_aq_auth_token(aq_username, aq_password, debug)
        if debug:
            print("Re-authenticated as {},  New token: {}.".format(aq_username,
                                                                   module_token))
        return module_token
    return token


def get_aquarius_location_timezone(loc_numeric_id, debug=False, token=module_token):
    # Verify the connection is still valid
    token_to_use = check_and_revalidate_connection(token, debug)

    location_dto = aq_client.service.GetLocation(
        loc_numeric_id, _soapheaders={"AQAuthToken": token_to_use})
    utc_offset_float = location_dto.UtcOffset
    utc_offset_string = '{:+3.0f}'.format(
        utc_offset_float * -1).strip()
    timezone = pytz.timezone('Etc/GMT' + utc_offset_string)
    if debug:
        print("Timezone for {} ({}): {}".format(
            location_dto.LocationId, location_dto.Identifier, timezone))
    return timezone


def get_aquarius_timezone(ts_numeric_id, loc_numeric_id=None, debug=False, token=module_token):
    # Verify the connection is still valid
    token_to_use = check_and_revalidate_connection(token, debug)

    if loc_numeric_id is None:
        all_locations = aq_client.service.GetAllLocations(
            _soapheaders={"AQAuthToken": token_to_use}).LocationDTO
    else:
        all_locations = []
        location_dto = aq_client.service.GetLocation(
            loc_numeric_id, _soapheaders={"AQAuthToken": token_to_use})
        all_locations.append(location_dto)
    for location in all_locations:
        utc_offset_float = location.UtcOffset
        utc_offset_string = '{:+3.0f}'.format(
            utc_offset_float * -1).strip()
        timezone = pytz.timezone('Etc/GMT' + utc_offset_string)
        all_descriptions_array = aq_client.service.GetTimeSeriesListForLocation(location.LocationId, _soapheaders={"AQAuthToken": token_to_use}
                                                                                )
        try:
            all_descriptions = all_descriptions_array.TimeSeriesDescription
        except AttributeError:
            pass
        else:
            for description in all_descriptions:
                ts_id = description.AqDataID
                if ts_id == ts_numeric_id:
                    return timezone


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
        csv_data = data_table.to_csv(header=False, index=False,
                                     columns=['AQLocalizedTimeStamp', 'data_value', 'flag',
                                              'grade', 'interpolation', 'approval', 'note'],
                                     date_format='%Y-%m-%d %H:%M:%S')

        # convert str to bytes
        byte_string = bytes(csv_data, 'ascii')
        # Convert the data string into a base64 object
        # csv_bytes = base64.b64encode(byte_string).decode('ascii')
        csv_bytes = base64.b64encode(byte_string)

    else:
        csv_bytes = ""

    # return csv_bytes
    return byte_string


def aq_timeseries_append(ts_numeric_id, appendbytes, debug=False, token=module_token):
    """
    Appends data to an aquarius time series given a base64 encoded csv string with the following values:
        datetime(isoformat), value, flag, grade, interpolation, approval, note
    :param ts_numeric_id: The integer primary key of an aquarius time series
    :param appendbytes: Base64 csv string with ISO-datetime, value, flag, grade, interpolation, approval, note
    :param debug: Says whether or not to issue print(statements.)
    :param token: A cookie jar with the current authentication cookie to the API client
    :return: The append result from the SOAP client
    """

    # Create an empty resute
    # empty_result = aq_client.factory.create('ns0:AppendResult')
    empty_result = aq_client.get_type(
        '{http://schemas.datacontract.org/2004/07/AQAcquisitionService.Dto}AppendResult')

    # Actually append to the Aquarius dataset
    t3 = datetime.datetime.now()
    if len(appendbytes) > 0:
        for attempt in range(10):
            try:
                # Verify the connection is still valid
                token_to_use = check_and_revalidate_connection(token, debug)
                append_result = aq_client.service.AppendTimeSeriesFromBytes2(
                    ts_numeric_id, appendbytes, aq_username, _soapheaders={"AQAuthToken": token_to_use})
                if debug:
                    print("Append result: {}".format(append_result))
                if pd.notna(append_result.AppendToken):
                    break
            except suds.WebFault as e:
                if debug:
                    print("      Error: {}".format(sys.exc_info()[0]))
                    print('      {}'.format(e))
                    print('      Retrying in 30 seconds')
                time.sleep(30)
                #     t4 = datetime.datetime.now()
                #     print("      API execution took {}".format(t4 - t3))
                # empty_result.NumPointsAppended = 0
                # empty_result.AppendToken = 0
                # empty_result.TsIdentifier = '\"Error: \"{0}\"'.format(
                #     str(e))
                # append_result = empty_result
                # break
            except socket.timeout as e:
                if debug:
                    print("      Socket timeout: {}".format(sys.exc_info()[0]))
                    print('      {}'.format(e))
                    print('      Retrying in 30 seconds')
                time.sleep(30)
            except Exception as e:
                if debug:
                    print("      Error: {}".format(sys.exc_info()[0]))
                    print('      {}'.format(e))
                    print('      Retrying in 30 seconds')
                time.sleep(30)
        else:  # if we never got to the "break" for a successful result
            if debug:
                print("      Error: {}".format(sys.exc_info()[0]))
                print('      10 retries attempted')
                t4 = datetime.datetime.now()
                print("      API execution took {}".format(t4 - t3))
            empty_result.NumPointsAppended = 0
            empty_result.AppendToken = 0
            empty_result.TsIdentifier = '\"Error: \"{0}\"'.format(
                sys.exc_info()[0])
            append_result = empty_result
    else:
        if debug:
            print("      No data appended from this query.")
        empty_result.NumPointsAppended = 0
        empty_result.AppendToken = 0
        empty_result.TsIdentifier = ""
        append_result = empty_result

    return append_result


def export_data_by_month(chunk_of_data, data_column, timeseries_id_numeric, debug=False, token=module_token):
    # Output a CSV
    csv_data = chunk_of_data.rename(
        columns={data_column: 'data_value'}).dropna(
        axis=0, subset=['data_value']).reindex(columns=['AQLocalizedTimeStamp', 'data_value', 'flag',
                                                        'grade', 'interpolation', 'approval', 'note']
                                               ).to_csv(
        header=False, index=False,
        date_format='%Y-%m-%d %H:%M:%S'
    )
    # print(csv_data[0:60])
    # convert str to bytes
    byte_string = bytes(csv_data, 'ascii')
    # Convert the data string into a base64 object
    csv_bytes = base64.b64encode(byte_string).decode('ascii')
    result = aq_timeseries_append(
        timeseries_id_numeric, csv_bytes, False, token=token)
    if result.NumPointsAppended > 0:
        print("Year: {}    Month {}".format(
            chunk_of_data.index.year[0], chunk_of_data.index.month[0]))
        print(result)
        time.sleep(15)
