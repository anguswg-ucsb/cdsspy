import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

def get_call_analysis_wdid(
    wdid                = None,
    admin_no            = None,
    start_date          = None,
    end_date            = None,
    batch               = False,
    api_key             = None
    ):
    """Return call analysis by WDID from analysis services API
    
    Makes a request to the analysisservices/callanalysisbywdid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified WDID and priority was out of priority and the downstream call in priority.
    
    Args:
        wdid (str, optional): DWR WDID unique structure identifier code. Defaults to None.
        admin_no (str, int optional): Water Right Administration Number. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        batch (bool, optional): Boolean, whether to break date range calls into batches of 1 year. This can speed up data retrieval for date ranges greater than a year. A date range of 5 years would be batched into 5 separate API calls for each year. Default is False, will run a single query for the entire date range.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.
    Returns:
        pandas dataframe object: dataframe of call services by WDID
    """
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # convert int admin_no to str
    if(isinstance(admin_no, (int))):
        admin_no = str(admin_no)
    
    # if function should be run in batch mode
    if(batch == True):

        # final output dataframe to append query results to
        out_df = pd.DataFrame()
        
        # make a list of date ranges to issue GET requests in smaller batches
        date_lst = utils._batch_dates(
            start_date = start_date,
            end_date   = end_date
            )
        
        # print message 
        print("Retrieving call analysis data by WDID")

        # go through range of dates in date_df and make batch GET requests
        for idx, val in enumerate(date_lst):

            print("Batch: ", idx+1, "/", len(date_lst))
            
            cdss_df = _inner_call_analysis_wdid(
                wdid       = wdid,
                admin_no   = admin_no,
                start_date = val[0],
                end_date   = val[1],
                api_key    = api_key
                )
            
            # bind data from this page
            out_df = pd.concat([out_df, cdss_df])
        
        return out_df
    
    else:

        # print message 
        print("Retrieving call analysis data by WDID")

        out_df = _inner_call_analysis_wdid(
            wdid       = wdid,
            admin_no   = admin_no,
            start_date = start_date,
            end_date   = end_date,
            api_key    = api_key
            )
        
        return out_df

def _inner_call_analysis_wdid(
    wdid                = None,
    admin_no            = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return call analysis by WDID from analysis services API
    
    Makes a request to the analysisservices/callanalysisbywdid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified WDID and priority was out of priority and the downstream call in priority.
    
    Args:
        wdid (str, optional): DWR WDID unique structure identifier code. Defaults to None.
        admin_no (str, int optional): Water Right Administration Number. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of call services by WDID
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [wdid, admin_no]):
    #     raise TypeError("Invalid 'wdid' and 'admin_no' parameters.\nPlease enter a 'wdid' and 'admin_no' to retrieve call analysis data")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # convert int admin_no to str
    if(isinstance(admin_no, (int))):
        admin_no = str(admin_no)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/callanalysisbywdid/?"
    
    # parse start_date into query string format
    start = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
        )

    # parse end_date into query string format
    end = utils._parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&adminNo={admin_no or ""}'
            f'&endDate={end or ""}'
            f'&startDate={start or ""}'
            f'&wdid={wdid or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )
        
        # # make API call w/ error handling
        # cdss_req = _get_error_handler(
        #     url      = url
        #     )

        # extract dataframe from list column
        cdss_df = cdss_req.json()
        cdss_df = pd.DataFrame(cdss_df)
        cdss_df = cdss_df["ResultList"].apply(pd.Series)

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])

        # Check if more pages to get to continue/stop while loop
        if len(cdss_df.index) < page_size:
            more_pages = False
        else:
            page_index += 1

    return data_df

def _inner_call_analysis_gnisid(
    gnis_id             = None,
    admin_no            = None,
    stream_mile         = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return call analysis by GNIS ID from analysis services API
    
    Makes a request to the analysisservices/callanalysisbygnisid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified stream/stream mile and priority was out of priority and the downstream call in priority. 
    This can be used when there is not an existing WDID to be analyzed.

    Args:
        gnis_id(str): GNIS ID to query. Defaults to None.
        admin_no (str, int): Water Right Administration Number. Defaults to None.
        stream_mile (str, int, float): stream mile for call analysis. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of call services by GNIS ID
    """
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # convert int admin_no to str
    if(isinstance(admin_no, (int))):
        admin_no = str(admin_no)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/callanalysisbygnisid/?"
    
    # parse start_date into query string format
    start = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
        )

    # parse end_date into query string format
    end = utils._parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&adminNo={admin_no or ""}'
            f'&endDate={end or ""}'
            f'&gnisId={gnis_id or ""}'
            f'&startDate={start or ""}'
            f'&streamMile={stream_mile or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )
        
        # # make API call w/ error handling
        # cdss_req = _get_error_handler(
        #     url      = url
        #     )

        # extract dataframe from list column
        cdss_df = cdss_req.json()
        cdss_df = pd.DataFrame(cdss_df)
        cdss_df = cdss_df["ResultList"].apply(pd.Series)

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])

        # Check if more pages to get to continue/stop while loop
        if len(cdss_df.index) < page_size:
            more_pages = False
        else:
            page_index += 1

    return data_df

def get_call_analysis_gnisid(
    gnis_id             = None,
    admin_no            = None,
    stream_mile         = None,
    start_date          = None,
    end_date            = None,
    batch               = False,
    api_key             = None
    ):
    """Return call analysis by GNIS ID from analysis services API
    
    Makes a request to the analysisservices/callanalysisbygnisid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified stream/stream mile and priority was out of priority and the downstream call in priority. 
    This can be used when there is not an existing WDID to be analyzed.

    Args:
        gnis_id(str): GNIS ID to query. Defaults to None.
        admin_no (str, int): Water Right Administration Number. Defaults to None.
        stream_mile (str, int, float): stream mile for call analysis. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        batch (bool, optional): Boolean, whether to break date range calls into batches of 1 year. This can speed up data retrieval for date ranges greater than a year. A date range of 5 years would be batched into 5 separate API calls for each year. Default is False, will run a single query for the entire date range.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.
    Returns:
        pandas dataframe object: dataframe of call services by GNIS ID
    """
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # convert int admin_no to str
    if(isinstance(admin_no, (int))):
        admin_no = str(admin_no)
    
    # if function should be run in batch mode
    if(batch == True):

        # final output dataframe to append query results to
        out_df = pd.DataFrame()
        
        # make a list of date ranges to issue GET requests in smaller batches
        date_lst = utils._batch_dates(
            start_date = start_date,
            end_date   = end_date
            )
        
        # print message 
        print("Retrieving call analysis data by GNIS ID")

        # go through range of dates in date_df and make batch GET requests
        for idx, val in enumerate(date_lst):

            print("Batch: ", idx+1, "/", len(date_lst))

            cdss_df = _inner_call_analysis_gnisid(
                gnis_id      = gnis_id,
                admin_no     = admin_no,
                stream_mile  = stream_mile,
                start_date   = val[0],
                end_date     = val[1],
                api_key      = api_key
                )
            
            # bind data from this page
            out_df = pd.concat([out_df, cdss_df])
        
        return out_df
    
    else:

        # print message 
        print("Retrieving call analysis data by GNIS ID")

        out_df = _inner_call_analysis_gnisid(
            gnis_id      = gnis_id,
            admin_no     = admin_no,
            stream_mile  = stream_mile,
            start_date   = start_date,
            end_date     = end_date,
            api_key      = api_key
            )
        
        return out_df
    

def get_source_route_framework(
    division            = None,
    gnis_name           = None,
    water_district      = None,
    api_key             = None
    ):
    """Return call analysis by WDID from analysis services API

    Makes a request to the analysisservices/watersourcerouteframework/ endpoint to retrieve the DWR source route framework reference table for the criteria specified.

    Args:
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        gnis_name (str, optional): GNIS Name to query and retrieve DWR source route frameworks. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of source route framework
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [division, gnis_name, water_district]):
    #     raise TypeError("Invalid 'division', 'gnis_name' or 'water_district' parameters.\nPlease enter a 'division', 'gnis_name' or 'water_district' to retrieve  DWR source route framework data")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/watersourcerouteframework/?"
    
    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving DWR source route frameworks")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&division={division or ""}'
            f'&gnisName={gnis_name or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df = cdss_req.json()
        cdss_df = pd.DataFrame(cdss_df)
        cdss_df = cdss_df["ResultList"].apply(pd.Series)

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])

        # Check if more pages to get to continue/stop while loop
        if len(cdss_df.index) < page_size:
            more_pages = False
        else:
            page_index += 1

    return data_df

def get_source_route_analysis(
    lt_gnis_id          = None,
    lt_stream_mile      = None,
    ut_gnis_id          = None,
    ut_stream_mile      = None,
    api_key             = None
    ):
    """Returns all WDID(s), and their stream mile, located between two different stream locations on the DWR Water Source Framework

    Makes a request to the analysisservices/watersourcerouteanalysis/ endpoint to retrieve the DWR source route framework analysis data.
    Args:
        lt_gnis_id (str, int):  lower terminus GNIS ID. Defaults to None.
        lt_stream_mile (str, int): lower terminus stream mile. Defaults to None.
        ut_gnis_id (str, int):  upper terminus GNIS ID. Defaults to None.
        ut_stream_mile (str, int): upper terminus stream mile. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water source route framework analysis
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/watersourcerouteanalysis/?"
    
    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving DWR source route analysis")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&ltGnisId={lt_gnis_id or ""}'
            f'&ltStreamMile={lt_stream_mile or ""}'
            f'&utGnisId={ut_gnis_id or ""}'
            f'&utStreamMile={ut_stream_mile or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df = cdss_req.json()
        cdss_df = pd.DataFrame(cdss_df)
        cdss_df = cdss_df["ResultList"].apply(pd.Series)

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])

        # Check if more pages to get to continue/stop while loop
        if len(cdss_df.index) < page_size:
            more_pages = False
        else:
            page_index += 1

    return data_df

# def get_call_analysis_wdid(
#     wdid                = None,
#     admin_no            = None,
#     start_date          = None,
#     end_date            = None,
#     api_key             = None
#     ):
#     """Return call analysis by WDID from analysis services API
    
#     Makes a request to the analysisservices/callanalysisbywdid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified WDID and priority was out of priority and the downstream call in priority.
    
#     Args:
#         wdid (str, optional): DWR WDID unique structure identifier code. Defaults to None.
#         admin_no (str, int optional): Water Right Administration Number. Defaults to None.
#         start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
#         end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
#         api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

#     Returns:
#         pandas dataframe object: dataframe of call services by WDID
#     """
    
#     # list of function inputs
#     input_args = locals()

#     # check function arguments for missing/invalid inputs
#     arg_lst = utils._check_args(
#         arg_dict = input_args,
#         ignore   = ["api_key", "start_date", "end_date"],
#         f        = any
#         )
    
#     # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
#     if arg_lst is not None:
#         raise Exception(arg_lst)
    
#     # convert int admin_no to str
#     if(isinstance(admin_no, (int))):
#         admin_no = str(admin_no)

#     #  base API URL
#     base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/callanalysisbywdid/?"

#     # make a list of date ranges to issue GET requests in smaller batches
#     date_lst = utils._batch_dates(
#         start_date = start_date,
#         end_date   = end_date
#         )

#     # final output dataframe to append query results to
#     out_df = pd.DataFrame()

#     # print message 
#     print("Retrieving call analysis data by WDID")

#     # go through range of dates in date_df and make batch GET requests
#     for idx, val in enumerate(date_lst):

#         print("index: ", idx, " | value: ", val)
#         # print("START: ", val[0], " | END: ", val[1])

#         # parse start_date into query string format
#         start = utils._parse_date(
#             date   = val[0],
#             start  = True,
#             format = "%m-%d-%Y"
#             )

#         # parse end_date into query string format
#         end = utils._parse_date(
#             date   = val[1],
#             start  = False,
#             format = "%m-%d-%Y"
#             )

#         # maximum records per page
#         page_size = 50000

#         # initialize empty dataframe to store data from multiple pages
#         data_df = pd.DataFrame()

#         # initialize first page index
#         page_index = 1

#         # Loop through pages until there are no more pages to get
#         more_pages = True

#         # Loop through pages until last page of data is found, binding each response dataframe together
#         while more_pages == True:

#             # create query URL string
#             url = (
#                 f'{base}format=json&dateFormat=spaceSepToSeconds'
#                 f'&adminNo={admin_no or ""}'
#                 f'&endDate={end or ""}'
#                 f'&startDate={start or ""}'
#                 f'&wdid={wdid or ""}'
#                 f'&pageSize={page_size}&pageIndex={page_index}'
#                 )

#             # If an API key is provided, add it to query URL
#             if api_key is not None:
#                 # Construct query URL w/ API key
#                 url = url + "&apiKey=" + str(api_key)

#             # make API call w/ error handling
#             cdss_req = utils._parse_gets(
#                 url      = url, 
#                 arg_dict = input_args,
#                 ignore   = None
#                 )
            
#             # # make API call w/ error handling
#             # cdss_req = _get_error_handler(
#             #     url      = url
#             #     )

#             # extract dataframe from list column
#             cdss_df = cdss_req.json()
#             cdss_df = pd.DataFrame(cdss_df)
#             cdss_df = cdss_df["ResultList"].apply(pd.Series)

#             # bind data from this page
#             data_df = pd.concat([data_df, cdss_df])

#             # Check if more pages to get to continue/stop while loop
#             if len(cdss_df.index) < page_size:
#                 more_pages = False
#             else:
#                 page_index += 1

#         # bind data from this page
#         out_df = pd.concat([out_df, data_df])

#     return out_df


# def get_call_analysis_wdid(
#     wdid                = None,
#     admin_no            = None,
#     start_date          = None,
#     end_date            = None,
#     api_key             = None
#     ):
#     """Return call analysis by WDID from analysis services API
    
#     Makes a request to the analysisservices/callanalysisbywdid/ endpoint that performs a call analysis that returns a time series showing the percentage of each day that the specified WDID and priority was out of priority and the downstream call in priority.
    
#     Args:
#         wdid (str, optional): DWR WDID unique structure identifier code. Defaults to None.
#         admin_no (str, int optional): Water Right Administration Number. Defaults to None.
#         start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
#         end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
#         api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

#     Returns:
#         pandas dataframe object: dataframe of call services by WDID
#     """
    
#     # list of function inputs
#     input_args = locals()

#     # check function arguments for missing/invalid inputs
#     arg_lst = utils._check_args(
#         arg_dict = input_args,
#         ignore   = ["api_key", "start_date", "end_date"],
#         f        = any
#         )
    
#     # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
#     if arg_lst is not None:
#         raise Exception(arg_lst)
    
#     # convert int admin_no to str
#     if(isinstance(admin_no, (int))):
#         admin_no = str(admin_no)

#     #  base API URL
#     base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/callanalysisbywdid/?"

#     # make a list of date ranges to issue GET requests in smaller batches
#     date_lst = utils._batch_dates(
#         start_date = start_date,
#         end_date   = end_date
#         )

#     # final output dataframe to append query results to
#     out_df = pd.DataFrame()

#     # print message 
#     print("Retrieving call analysis data by WDID")

#     # go through range of dates in date_df and make batch GET requests
#     for idx, val in enumerate(date_lst):

#         print("index: ", idx, " | value: ", val)
#         # print("START: ", val[0], " | END: ", val[1])

#         # parse start_date into query string format
#         start = utils._parse_date(
#             date   = val[0],
#             start  = True,
#             format = "%m-%d-%Y"
#             )

#         # parse end_date into query string format
#         end = utils._parse_date(
#             date   = val[1],
#             start  = False,
#             format = "%m-%d-%Y"
#             )

#         # maximum records per page
#         page_size = 50000

#         # initialize empty dataframe to store data from multiple pages
#         data_df = pd.DataFrame()

#         # initialize first page index
#         page_index = 1

#         # Loop through pages until there are no more pages to get
#         more_pages = True

#         # Loop through pages until last page of data is found, binding each response dataframe together
#         while more_pages == True:

#             # create query URL string
#             url = (
#                 f'{base}format=json&dateFormat=spaceSepToSeconds'
#                 f'&adminNo={admin_no or ""}'
#                 f'&endDate={end or ""}'
#                 f'&startDate={start or ""}'
#                 f'&wdid={wdid or ""}'
#                 f'&pageSize={page_size}&pageIndex={page_index}'
#                 )

#             # If an API key is provided, add it to query URL
#             if api_key is not None:
#                 # Construct query URL w/ API key
#                 url = url + "&apiKey=" + str(api_key)

#             # make API call w/ error handling
#             cdss_req = utils._parse_gets(
#                 url      = url, 
#                 arg_dict = input_args,
#                 ignore   = None
#                 )
            
#             # # make API call w/ error handling
#             # cdss_req = _get_error_handler(
#             #     url      = url
#             #     )

#             # extract dataframe from list column
#             cdss_df = cdss_req.json()
#             cdss_df = pd.DataFrame(cdss_df)
#             cdss_df = cdss_df["ResultList"].apply(pd.Series)

#             # bind data from this page
#             data_df = pd.concat([data_df, cdss_df])

#             # Check if more pages to get to continue/stop while loop
#             if len(cdss_df.index) < page_size:
#                 more_pages = False
#             else:
#                 page_index += 1

#         # bind data from this page
#         out_df = pd.concat([out_df, data_df])

#     return out_df