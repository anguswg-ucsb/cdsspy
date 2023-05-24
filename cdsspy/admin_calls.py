import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

def get_admin_calls(
    division            = None,
    location_wdid       = None,
    call_number         = None,
    start_date          = None,
    end_date            = None,
    active              = True,
    api_key             = None
    ):
    """Return active/historic administrative calls data

    Make a request to the api/v2/administrativecalls endpoints to locate active or historical administrative calls by division, location WDID, or call number within a specified date range.

    Args:
        division (int, str, optional): Water division to query for administrative calls. Defaults to None.
        location_wdid (str, optional): call location structure WDID to query for administrative calls. Defaults to None.
        call_number (int, str, optional): unique call identifier to query. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        active (bool, optional): whether to get active or historical administrative calls. Defaults to True which returns active administrative calls.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of active/historical administrative calls data
    """
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date", "active"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # collapse location_wdid list, tuple, vector of site_id into query formatted string
    location_wdid = utils._collapse_vector(
        vect = location_wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = utils._parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    #  base API URL and print statements
    if active == True:
        print("Retrieving active administrative calls data")
        base = "https://dwr.state.co.us/Rest/GET/api/v2/administrativecalls/active/?"
    else:
        print("Retrieving historical administrative calls data")
        base = "https://dwr.state.co.us/Rest/GET/api/v2/administrativecalls/historical/?"

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving surface water station data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:
        
        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&min-dateTimeSet={start_date or ""}' 
            f'&max-dateTimeSet={end_date or ""}'
            f'&division={division or ""}' 
            f'&callNumber={call_number or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # Construct query URL w/ location WDID
        if location_wdid is not None:
            url = url + "&locationWdid=" + str(location_wdid)

        # If an API key is provided, add it to query URL
        if api_key is not None:
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
        if (len(cdss_df.index) < page_size):
            more_pages = False
        else:
            page_index += 1
    
    return data_df