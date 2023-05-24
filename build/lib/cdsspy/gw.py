import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

def get_gw_wl_wells(
    county              = None,
    designated_basin    = None,
    division            = None,
    management_district = None,
    water_district      = None,
    wellid              = None,
    api_key             = None
    ):
    """Search for groundwater water level wells
    
    Make a request to the groundwater/waterlevels/wells endpoint to retrieve groundwater water level wells data.

    Args:
        county (str, optional): County to query for groundwater water level wells. Defaults to None.
        designated_basin (str, optional): Designated basin to query for groundwater water level wells. Defaults to None.
        division (str, optional): Division to query for groundwater water level wells. Defaults to None.
        management_district (str, optional): Management district to query for groundwater water level wells. Defaults to None.
        water_district (str, optional): Water district to query for groundwater water level wells. Defaults to None.
        wellid (str, optional): Well ID of a groundwater water level well. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater water level wells
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore  = ["api_key"],
        f       = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/groundwater/waterlevels/wells/?"

    # if county is given, make sure it is separated by "+" and all uppercase 
    if county is not None:
        county = county.replace(" ", "+")
        county = county.upper()

    # if management_district is given, make sure it is separated by "+" and all uppercase 
    if management_district is not None:
        management_district = management_district.replace(" ", "+")
        management_district = management_district.upper()

    # if designated_basin is given, make sure it is separated by "+" and all uppercase 
    if designated_basin is not None:
        designated_basin = designated_basin.replace(" ", "+")
        designated_basin = designated_basin.upper()

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving groundwater water level data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}' 
            f'&wellId={wellid or ""}'
            f'&division={division or ""}' 
            f'&waterDistrict={water_district or ""}' 
            f'&designatedBasin={designated_basin or ""}' 
            f'&managementDistrict={management_district or ""}' 
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

def get_gw_wl_wellmeasures(
    wellid        = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return groundwater water level well measurements

    Make a request to the groundwater/waterlevels/wellmeasurements endpoint to retrieve groundwater water level well measurement data.

    Args:
        wellid (str): Well ID to query for groundwater water level measurements. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater well measurements
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/groundwater/waterlevels/wellmeasurements/?"

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

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving groundwater water level measurements")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&min-measurementDate={start_date or ""}' 
            f'&min-measurementDate={end_date or ""}'
            f'&wellId={wellid or ""}'
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

def get_gw_gplogs_wells(
    county              = None,
    designated_basin    = None,
    division            = None,
    management_district = None,
    water_district      = None,
    wellid              = None,
    api_key             = None
    ):
    """Search for groundwater geophysicallog wells
    
    Make a request to the groundwater/geophysicallogs/wells endpoint to retrieve groundwater geophysicallog wells data.
    
    Args:
        county (str, optional): County to query for groundwater geophysicallog wells. Defaults to None.
        designated_basin (str, optional): Designated basin to query for groundwater geophysicallog wells. Defaults to None.
        division (str, optional): Division to query for groundwater geophysicallog wells. Defaults to None.
        management_district (str, optional): Management district to query for groundwater geophysicallog wells. Defaults to None.
        water_district (str, optional): Water district to query for groundwater geophysicallog wells. Defaults to None.
        wellid (str, optional): Well ID of a groundwater geophysicallog wells. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater geophysicallog wells
    """

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
    base = "https://dwr.state.co.us/Rest/GET/api/v2/groundwater/geophysicallogs/wells/?"

    # if county is given, make sure it is separated by "+" and all uppercase 
    if county is not None:
        county = county.replace(" ", "+")
        county = county.upper()

    # if management_district is given, make sure it is separated by "+" and all uppercase 
    if management_district is not None:
        management_district = management_district.replace(" ", "+")
        management_district = management_district.upper()

    # if designated_basin is given, make sure it is separated by "+" and all uppercase 
    if designated_basin is not None:
        designated_basin = designated_basin.replace(" ", "+")
        designated_basin = designated_basin.upper()

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving groundwater geophysicallog wells data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:
        
        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}' 
            f'&wellId={wellid or ""}'
            f'&division={division or ""}' 
            f'&waterDistrict={water_district or ""}' 
            f'&designatedBasin={designated_basin or ""}' 
            f'&managementDistrict={management_district or ""}' 
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

def get_gw_gplogs_geologpicks(
    wellid              = None,
    api_key             = None
    ):
    """Return Groundwater Geophysical Log picks by well ID

    Make a request to the groundwater/geophysicallogs/wells endpoint to retrieve groundwater geophysical log picks for the given well ID.
    
    Args:
        wellid (str, optional): Well ID of a groundwater geophysicallog wells. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater geophysical log picks
    """

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
    base = "https://dwr.state.co.us/Rest/GET/api/v2/groundwater/geophysicallogs/geoplogpicks/?"

    # If no well ID is provided
    if wellid is None:
        return print("Invalid 'wellid' parameter")

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving groundwater geophysical log picks data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wellId={wellid or ""}'
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