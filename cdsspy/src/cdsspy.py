# __init__.py
__version__ = "1.2.3"

import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

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
    # # If all inputs are None, then return error message
    # if all(i is None for i in [division, location_wdid, call_number]):
    #     raise TypeError("Invalid 'division', 'location_wdid', or 'call_number' parameters")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date", "active"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # collapse location_wdid list, tuple, vector of site_id into query formatted string
    location_wdid = _collapse_vector(
        vect = location_wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
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
        cdss_req = _parse_gets(
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

def get_climate_stations(
    aoi                 = None,
    radius              = None,
    county              = None,
    division            = None,
    station_name        = None,
    site_id             = None,
    water_district      = None,
    api_key             = None
    ):
    """Return Climate Station information

    Make a request to the climatedata/climatestations/ endpoint to locate climate stations via a spatial search, or by county, division, station name, Site ID or water district.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for climate stations. Defaults to None.
        division (int, str, optional):  Water division to query for climate stations. Defaults to None.
        station_name (str, optional):  climate station name. Defaults to None.
        site_id (str, tuple, list, optional): string, tuple or list of site IDs. Defaults to None.
        water_district (int, str, optional): Water district to query for climate stations. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.


    Returns:
        pandas dataframe object: dataframe of climate station data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, station_name, site_id, water_district]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'station_name', 'site_id', or 'water_district' parameters")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse site_id list, tuple, vector of site_id into query formatted string
    site_id = _collapse_vector(
        vect = site_id, 
        sep  = "%2C+"
        )

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestations/?"

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving climate station data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}' 
            f'&division={division or ""}'
            f'&stationName={station_name or ""}' 
            f'&siteId={site_id or ""}'
            f'&waterDistrict={water_district or ""}' 
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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
    
    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )

    return data_df

def get_climate_frostdates(
    station_number      = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return climate stations frost dates 

    Make a request to the /climatedata/climatestationfrostdates endpoint to retrieve climate stations frost dates data by station number within a given date range (start and end dates)

    Args:
        station_number (str, optional): climate data station number. Defaults to None.
        start_date (str, optional): date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station frost dates data
    """
    # # If all inputs are None, then return error message
    # if all(i is None for i in [station_number]):
    #     raise TypeError("Invalid 'station_number' parameter")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationfrostdates/?"
    
    # parse start_date into query string format
    start_year = _parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_year = _parse_date(
        date   = end_date,
        start  = False,
        format = "%Y"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving climate station frost dates data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&min-calYear={start_year or ""}' 
            f'&max-calYear={end_year or ""}'
            f'&stationNum={station_number or ""}' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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


def _get_climate_ts_day(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return daily climate data
    
    Make a request to the /climatedata/climatestationtsday endpoint to retrieve climate stations daily time series data by station number, or Site IDs within a given date range (start and end dates)
    
    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        site_id (str, tuple, list, optional): string, tuple or list of climate station site IDs. Defaults to None.
        param (str):  climate variable. One of: "Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow", "SnowDepth", "SnowSWE", "Solar","VP", "Wind". Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station daily time series data
    """

    # list of valid parameters
    param_lst = ["Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow","SnowDepth", "SnowSWE", "Solar","VP", "Wind"]

    # if parameter is not in list of valid parameters
    if param not in param_lst:
        raise ValueError("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # # If all inputs are None, then return error message
    # if all(i is None for i in [site_id, station_number]):
    #     raise TypeError("Invalid 'site_id' or 'station_number' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_dict)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationtsday/?"

    # collapse list, tuple, vector of site_id into query formatted string
    site_id = _collapse_vector(
        vect = site_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print(f"Retrieving daily climate time series data ({param})")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&min-measDate={start_date or ""}' 
            f'&max-measDate={end_date or ""}'
            f'&stationNum={station_number or ""}' 
            f'&siteId={site_id or ""}'
            f'&measType={param or ""}' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # convert measDate columns to 'date' and pd datetime type
        cdss_df['measDate'] = pd.to_datetime(cdss_df['measDate'])

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1
    
    return data_df

def _get_climate_ts_month(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return monthly climate data
    
    Make a request to the /climatedata/climatestationtsmonth endpoint to retrieve climate stations monthly time series data by station number, or Site IDs within a given date range (start and end dates)
    
    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        site_id (str, optional):  tuple or list of climate station site IDs. Defaults to None.
        param (str, optional):  climate variable. One of: "Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow", "SnowDepth", "SnowSWE", "Solar","VP", "Wind". Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station monthly time series data
    """
    # list of valid parameters
    param_lst = ["Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow","SnowDepth", "SnowSWE", "Solar","VP", "Wind"]

    # if parameter is not in list of valid parameters
    if param not in param_lst:
        raise ValueError("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # # If all inputs are None, then return error message
    # if all(i is None for i in [site_id, station_number]):
    #     raise TypeError("Invalid 'site_id' or 'station_number' parameters")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationtsmonth/?"

    # collapse list, tuple, vector of site_id into query formatted string
    site_id = _collapse_vector(
        vect = site_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%Y"
        )

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print(f"Retrieving monthly climate time series data ({param})")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&min-calYear={start_date or ""}'
            f'&max-calYear={end_date or ""}'
            f'&stationNum={station_number or ""}' 
            f'&siteId={site_id or ""}' 
            f'&measType={param or ""}' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # convert string month to have leading 0 if month < 10
        cdss_df['month_str'] = cdss_df["calMonthNum"]

        # add month w/ leading 0 column
        cdss_df.loc[(cdss_df['calMonthNum'] < 10), 'month_str'] = "0" + cdss_df["calMonthNum"].astype(str)

        # create datetime column w/ calYear and month_str columns, and convert to pd datetime type
        cdss_df["datetime"] = pd.to_datetime(cdss_df['calYear'].astype(str) + "-" + cdss_df["month_str"].astype(str) + "-01")
    
        # drop month_str column
        cdss_df = cdss_df.drop('month_str', axis = 1)

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1
    
    return data_df

def get_climate_ts(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    timescale           = None,
    api_key             = None
    ):

    """Return climate station time series data

    Make a request to the /climatedata/climatestationts endpoints to retrieve daily or monthly (climatestationtsday or climatestationtsmonth)climate station time series data by station number or Site IDs within a given date range (start and end dates)
    
    Args:
        station_number (str, optional): climate data station number. Defaults to None.
        site_id (str, optional): string, tuple or list of climate station site IDs. Defaults to None.
        param (str, optional): climate variable. One of: "Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow", "SnowDepth", "SnowSWE", "Solar","VP", "Wind". Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): timestep of the time series data to return, either "day" or "month". Defaults to None and will request daily time series.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [site_id, station_number]):
    #     raise TypeError("Invalid 'site_id' or 'station_number' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore  = ["api_key", "start_date", "end_date", "timescale"],
        f       = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    # lists of valid timesteps
    day_lst       = ['day', 'days', 'daily', 'd']
    month_lst     = ['month', 'months', 'monthly', 'mon', 'm']
    timescale_lst = day_lst + month_lst

    # if timescale is None, then defaults to "day"
    if timescale is None: 
        timescale = "day"
        
    # if parameter is NOT in list of valid parameters
    if timescale not in timescale_lst:
        raise ValueError(f"Invalid `timescale` argument: '{timescale}'\nPlease enter one of the following valid timescales: \n{day_lst}\n{month_lst}")

    # request daily climate time series data
    if timescale in day_lst:    
        clim_data = _get_climate_ts_day(
            station_number      = station_number,
            site_id             = site_id,
            param               = param,
            start_date          = start_date,
            end_date            = end_date,
            api_key             = api_key
            )

        # return daily climate time series data
        return clim_data

    # request monthly climate time series data
    if timescale in month_lst:    

        clim_data = _get_climate_ts_month(
            station_number      = station_number,
            site_id             = site_id,
            param               = param,
            start_date          = start_date,
            end_date            = end_date,
            api_key             = api_key
            )

        # return monthly climate time series data
        return clim_data

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
    # If all inputs are None, then return error message
    # if all(i is None for i in [county, designated_basin, division, management_district, water_district, wellid]):
        # raise TypeError("Invalid 'county', 'designated_basin', 'division', 'management_district', 'water_district', or 'wellid' parameters")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
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
        cdss_req = _parse_gets(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater well measurements
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [wellid]):
    #     raise TypeError("Invalid 'wellid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
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
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
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
        cdss_req = _parse_gets(
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

    # If all inputs are None, then return error message
    # if all(i is None for i in [county, designated_basin, division, management_district, water_district, wellid]):
        # raise TypeError("Invalid 'county', 'designated_basin', 'division', 'management_district', 'water_district', or 'wellid' parameters")
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
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
        cdss_req = _parse_gets(
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
    # # If all inputs are None, then return error message
    # if all(i is None for i in [wellid]):
    #     raise TypeError("Invalid 'wellid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
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
        cdss_req = _parse_gets(
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

def get_reference_tbl(
    table_name = None,
    api_key    = None
    ):
    """Return Reference Table reference table
    
    Makes requests to the /referencetables/ endpoints and returns helpful reference tables. Reference tables can help identify valid inputs for querying CDSS API resources using cdsspy.  
    For more detailed information visit: https://dwr.state.co.us/rest/get/help#Datasets&#ReferenceTablesController&#gettingstarted&#jsonxml.
    
    Args:
        table_name (str, optional): name of the reference table to return. Must be one of:
            ("county", "waterdistricts", "waterdivisions", "designatedbasins", "managementdistricts", "telemetryparams", "climateparams", "divrectypes", "flags"). Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.
    
    Returns:
        pandas dataframe: dataframe of CDSS reference tables
    """
    # list of valid parameters
    tbl_lst = ["county", "waterdistricts", "waterdivisions", "designatedbasins", "managementdistricts", "telemetryparams", "climateparams", "divrectypes", "flags"]

    # if parameter is not in list of valid parameters
    if table_name not in tbl_lst:
        raise ValueError("Invalid `table_name` argument \nPlease enter one of the following valid table names: \ncounty\nwaterdistricts\nwaterdivisions\ndesignatedbasins\nmanagementdistricts\ntelemetryparams\nclimateparams\ndivrectypes\nflags")

    # retrieve county reference table
    if table_name == "county":
        ref_table = _get_ref_county(
            api_key = api_key
            )
        return ref_table
    
    # retrieve water districts reference table
    if table_name == "waterdistricts":
        ref_table = _get_ref_waterdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve water divisions reference table
    if table_name == "waterdivisions":
        ref_table = _get_ref_waterdivisions(
            api_key = api_key
            )
        return ref_table

    # retrieve management districts reference table
    if table_name == "managementdistricts":
        ref_table = _get_ref_managementdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve designated basins reference table
    if table_name == "designatedbasins":
        ref_table = _get_ref_designatedbasins(
            api_key = api_key
            )
        return ref_table

    # retrieve telemetry station parameters reference table
    if table_name == "telemetryparams":
        ref_table = _get_ref_telemetry_params(
            api_key = api_key
            )
        return ref_table

    # retrieve climate station parameters reference table
    if table_name == "climateparams":
        ref_table = _get_ref_climate_params(
            api_key = api_key
            )
        return ref_table

    # retrieve diversion record types reference table
    if table_name == "divrectypes":
        ref_table = _get_ref_divrectypes(
            api_key = api_key
            )
        return ref_table

    # retrieve station flags reference table
    if table_name == "flags":
        ref_table = _get_ref_stationflags(
            api_key = api_key
            )
        return ref_table
        
def _get_ref_county(
    county  = None, 
    api_key = None
    ):
    """Return county reference table

    Args:
        county (str, optional): County to query, if no county is given, entire county dataframe is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado counties
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/county/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving reference table: Counties")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df


def _get_ref_waterdistricts(
    division       = None, 
    water_district = None,
    api_key        = None
    ):
    """Return water districts reference table

    Args:
        division (str, optional):  (optional) indicating the division to query, if no division is given, dataframe of all water districts is returned. Defaults to None.
        water_district (str, optional):  (optional) indicating the water district to query, if no water district is given, dataframe of all water districts is returned. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado water_districts
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/waterdistrict/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving reference table: Water districts")
    
    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&division={division or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_waterdivisions(
    division       = None, 
    api_key        = None
    ):
    """Return water divisions reference table

    Args:
        division (str, optional): Division to query, if no division is given, dataframe of all water divisions is returned. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado water divisions
    """

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/waterdivision/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving reference table: Water divisions")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&division={division or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_managementdistricts(
    management_district   = None, 
    api_key               = None
    ):
    """Return management districts reference table
    
    Args:
        management_district (str, optional): Indicating the management district to query, if no management district is given, dataframe of all management districts is returned Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado management districts
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/managementdistrict/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving reference table: Management districts")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&managementDistrictName={management_district or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_designatedbasins(
    designated_basin   = None, 
    api_key            = None
    ):
    """Return designated basin reference table
    
    Args:
        designated_basin (str, optional): Indicating the  designated basin to query character, if no designated basin is given, all designated basins dataframe is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado designated basins
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/designatedbasin/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving reference table: Designated basins")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&designatedBasinName={designated_basin or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_telemetry_params(
    param    = None, 
    api_key  = None
    ):
    """Return telemetry station parameter reference table
    
    Args:
        param (str, optional): Indicating the parameter to query character, if no parameter is given, all parameter dataframe is returned Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of telemetry station parameter reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/telemetryparams/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Telemetry station parameters")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json'
            f'&parameter={param or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_climate_params(
    param      = None, 
    api_key    = None
    ):
    """Return climate station parameter reference table
    
    Args:
        param (str, optional): Indicating the climate station parameter to query, if no parameter is given, all parameter dataframe is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of climate station parameter reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/climatestationmeastype/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Climate station parameters")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json'
            f'&measType={param or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_divrectypes(
    divrectype   = None, 
    api_key      = None
    ):
    """Return Diversion Record Types reference table
    
    Args:
        divrectype (str, optional): Diversion record type to query, if no divrectype is given, a dataframe with all diversion record types is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of diversion record types reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/divrectypes/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Diversion record types")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json'
            f'&divRecType={divrectype or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_ref_stationflags(
    flag    = None, 
    api_key = None
    ):
    """Return Station Flag reference table
    
    Args:
        flag (str, optional): short code for the flag to query, if no flag is given, a dataframe with all flags is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of diversion record types reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/stationflags/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Station flags")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json'
            f'&flag={flag or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def _get_structure_divrecday(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Daily Diversion/Release Records

    Make a request to the api/v2/structures/divrec/divrecday/ endpoint to retrieve daily structure diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure diversion/releases records 
    """

    # # if no wdid is given, return error
    # if wdid is None:
    #     raise TypeError("Invalid 'wdid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecday/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = _collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
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

    print(f'Retrieving daily structure {wc_identifier} data')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier=*{wc_identifier or ""}'
            f'*&min-dataMeasDate={start_date or ""}'
            f'&max-dataMeasDate={end_date or ""}'
            f'&wdid={wdid or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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


def _get_structure_divrecmonth(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Monthly Diversion/Release Records

    Make a request to the api/v2/structures/divrec/divrecmonth/ endpoint to retrieve monthly structure  diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of monthly structure diversion/releases records 
    """
    # # if no wdid is given, return error
    # if wdid is None:
    #     raise TypeError("Invalid 'wdid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecmonth/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = _collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%Y"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print(f'Retrieving monthly structure {wc_identifier} data')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier=*{wc_identifier or ""}'
            f'*&min-dataMeasDate={start_date or ""}'
            f'&max-dataMeasDate={end_date or ""}'
            f'&wdid={wdid or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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

def _get_structure_divrecyear(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Annual Diversion/Release Records

    Make a request to the structures/divrec/divrecyear/ endpoint to retrieve annual structure diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of annual structure diversion/releases records 
    """

    # # if no wdid is given, return error
    # if wdid is None:
    #     raise TypeError("Invalid 'wdid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecyear/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = _collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%Y"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print(f'Retrieving yearly structure {wc_identifier} data')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier=*{wc_identifier or ""}'
            f'*&min-dataMeasDate={start_date or ""}'
            f'&max-dataMeasDate={end_date or ""}'
            f'&wdid={wdid or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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

def get_structure_divrec_ts(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    timescale     = None, 
    api_key       = None
    ):

    """Return diversion/releases record data for administrative structures

    Make a request to the CDSS API /structures/divrec endpoints to get diversion/releases time series data for administrative structures by wdid, within a given date range (start and end dates) and at a specified temporal resolution.     

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): timestep of the time series data to return, either "day", "month", or "year". Defaults to None and will request daily time series.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of structure diversion/releases time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [wdid, wc_identifier]):
    #     raise TypeError("Invalid 'wdid' or 'wc_identifier' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date", "timescale"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    # lists of valid timesteps
    day_lst       = ['day', 'days', 'daily', 'd']
    month_lst     = ['month', 'months', 'monthly', 'mon', 'm']
    year_lst      = ['year', 'years', 'yearly', 'annual', 'annually', 'yr', 'y']
    timescale_lst = day_lst + month_lst + year_lst

    # if timescale is None, then defaults to "day"
    if timescale is None: 
        timescale = "day"
        
    # if parameter is NOT in list of valid parameters
    if timescale not in timescale_lst:
        raise ValueError(f"Invalid `timescale` argument: '{timescale}'\nPlease enter one of the following valid timescales: \n{day_lst}\n{month_lst}\n{year_lst}")

    # request daily structure divrec time series data
    if timescale in day_lst:    
        divrec_df = _get_structure_divrecday(
            wdid          = wdid,
            wc_identifier = wc_identifier,
            start_date    = start_date,
            end_date      = end_date,
            api_key       = api_key
            )

        # return daily climate time series data
        return divrec_df

    # request monthly structure divrec time series data
    if timescale in month_lst:    

        divrec_df = _get_structure_divrecmonth(
            wdid          = wdid,
            wc_identifier = wc_identifier,
            start_date    = start_date,
            end_date      = end_date,
            api_key       = api_key
            )

        # return monthly structure divrec time series data  
        return divrec_df

    # request yearly structure divrec time series data
    if timescale in year_lst:    

        divrec_df = _get_structure_divrecyear(
            wdid          = wdid,
            wc_identifier = wc_identifier,
            start_date    = start_date,
            end_date      = end_date,
            api_key       = api_key
            )

        # return yearly structure divrec time series data
        return divrec_df

def get_structure_stage_ts(
    wdid          = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return stage/volume record data for administrative structures

    Make a request to the structures/divrec/stagevolume/ endpoint to retrieve structure stage/volume data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):   optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure stage/volume records 
    """

    # # if no abbreviation is given, return error
    # if wdid is None:
    #     raise TypeError("Invalid 'wdid' parameter")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/stagevolume/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = _collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
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
            f'&min-dataMeasDate={start_date or ""}'
            f'&max-dataMeasDate={end_date or ""}'
            f'&wdid={wdid or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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

def get_structures(
    aoi            = None,
    radius         = None,
    county         = None,
    division       = None,
    gnis_id        = None,
    water_district = None,
    wdid           = None,
    api_key        = None
):
    """Return list of administrative structures

    Make a request to the api/v2/structures endpoint to locate administrative structures via a spatial search or by division, county, water_district, GNIS, or WDID.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): Indicating the county to query. Defaults to None.
        division (int, str, optional): Indicating the water division to query. Defaults to None.
        gnis_id (str, optional): Water source - Geographic Name Information System ID (GNIS ID). Defaults to None.
        water_district (int, str, optional): Indicating the water district to query. Defaults to None.
        wdid (str, tuple or list, optional): WDIDs code of structure. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of administrative structures
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, gnis_id, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'gnis_id', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/?"

    # convert numeric division to string
    if type(division) == int or type(division) == float:
        division = str(division)

    # convert numeric water_district to string
    if type(water_district) == int or type(water_district) == float:
        water_district = str(water_district)

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse WDID list, tuple, vector of site_id into query formatted string
    wdid = _collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
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
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&gnisId={gnis_id or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)
        
        # make API call w/ error handling
        cdss_req = _parse_gets(
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

    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )
    
    return data_df

def get_sw_stations(
    aoi                 = None,
    radius              = None,
    abbrev              = None,
    county              = None,
    division            = None,
    station_name        = None,
    usgs_id             = None,
    water_district      = None,
    api_key             = None
    ):
    """Return Surface Water Station information
    
    Make a request to the /surfacewater/surfacewaterstations endpoint to locate surface water stations via a spatial search, or by station abbreviation, county, division, station name, USGS ID or water_district.  

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        abbrev (str, list, tuple, optional): surface water station abbreviation. Defaults to None.
        county (str, optional): County to query for surface water stations. Defaults to None.
        division (int, str, optional):  Water division to query for surface water stations. Defaults to None.
        station_name (str, optional): surface water station name. Defaults to None.
        usgs_id (str, tuple or list , optional): USGS IDs. Defaults to None.
        water_district (int, str, optional): Water district to query for surface water stations. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.
    
    Returns:
        pandas dataframe object: dataframe of surface water station data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, abbrev, county, division, station_name, usgs_id, water_district]):
    #     raise TypeError("Invalid 'aoi', 'abbrev', 'county', 'division', 'station_name', 'usgs_id', or 'water_district' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse abbrev list, tuple, vector of abbrev into query formatted string
    abbrev = _collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse usgs_id list, tuple, vector of usgs_id into query formatted string
    usgs_id = _collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewaterstations/?"

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
            f'&abbrev={abbrev or ""}' 
            f'&county={county or ""}' 
            f'&division={division or ""}'
            f'&stationName={station_name or ""}' 
            f'&usgsSiteId={usgs_id or ""}'
            f'&waterDistrict={water_district or ""}' 
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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
    
    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )

    return data_df

def _get_sw_ts_day(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return daily surface water time series data
    
    Make a request to the /surfacewater/surfacewatertsday endpoint to retrieve surface water stations daily time series data by station abbreviations, station number, or USGS Site IDs within a given date range (start and end dates)
    
    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        abbrev (str, optional):  tuple or list of surface water station abbreviation. Defaults to None.
        station_number (int, str, optional):  surface water station number. Defaults to None.
        usgs_id (tuple, list, optional):  tuple or list of USGS ID. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: daily surface water time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [abbrev, station_number, usgs_id]):
    #     raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertsday/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = _collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = _collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving daily surface water time series")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&abbrev={abbrev or ""}'
            f'&min-measDate={start_date or ""}'
            f'&max-measDate={end_date or ""}'
            f'&stationNum={station_number or ""}'
            f'&usgsSiteId={usgs_id or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # convert measDate columns to 'date' and pd datetime type
        cdss_df['measDate'] = pd.to_datetime(cdss_df['measDate'])

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1
    
    return data_df

def _get_sw_ts_month(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return monthly surface water time series data
    
    Make a request to the /surfacewater/surfacewatertsmonth endpoint to retrieve surface water stations monthly time series data by station abbreviations, station number, or USGS Site IDs within a given date range (start and end dates)
    
    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        abbrev (tuple, list, optional):  tuple or list of surface water station abbreviation. Defaults to None.
        station_number (int, str, optional):  surface water station number. Defaults to None.
        usgs_id (str, optional):  tuple or list of USGS ID. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: monthly surface water time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [abbrev, station_number, usgs_id]):
    #     raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertsmonth/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = _collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = _collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%Y"
        )

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True 
    
    print("Retrieving monthly surface water time series")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&abbrev={abbrev or ""}'
            f'&min-calYear={start_date or ""}'
            f'&max-calYear={end_date or ""}'
            f'&stationNum={station_number or ""}'
            f'&usgsSiteId={usgs_id or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1
    
    return data_df

def _get_sw_ts_wyear(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return water year surface water time series data

    Make a request to the /surfacewater/surfacewatertswateryear endpoint to retrieve surface water stations annual time series data by station abbreviations, station number, or USGS Site IDs within a given date range (start and end dates)

    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        abbrev (str, optional):  tuple or list of surface water station abbreviation. Defaults to None.
        station_number (int, str, optional):  surface water station number. Defaults to None.
        usgs_id (str, optional):  tuple or list of USGS ID. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: annual surface water time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [abbrev, station_number, usgs_id]):
    #     raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertswateryear/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = _collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = _collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%Y"
        )

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving water year surface water time series")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&abbrev={abbrev or ""}'
            f'&min-waterYear={start_date or ""}'
            f'&max-waterYear={end_date or ""}'
            f'&stationNum={station_number or ""}'
            f'&usgsSiteId={usgs_id or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1
    
    return data_df

def get_sw_ts(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    timescale           = None,
    api_key             = None
    ):

    """Return surface water time series data
    
    Make a request to the /surfacewater/surfacewaterts/ endpoints (surfacewatertsday, surfacewatertsmonth, surfacewatertswateryear) to retrieve surface water station time series data by station abbreviations, station number, or USGS Site IDs within a given date range (start and end dates) and at a specified temporal resolution.     
    
    Args:
        station_number (str, optional):  climate data station number. Defaults to None.
        abbrev (str, optional):  tuple or list of surface water station abbreviation. Defaults to None.
        station_number (int, str, optional):  surface water station number. Defaults to None.
        usgs_id (tuple, list, optional):  tuple or list of USGS ID. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): timestep of the time series data to return, either "day", "month", or "water_year". Defaults to None and will request daily time series.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of surface water station time series data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [abbrev, station_number, usgs_id]):
    #     raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date", "timescale"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # lists of valid timesteps
    day_lst       = ['day', 'days', 'daily', 'd']
    month_lst     = ['month', 'months', 'monthly', 'mon', 'm']
    year_lst      = ['wyear', 'water_year', 'wyears', 'water_years', 'wateryear', 'wateryears', 'wy', 'year', 'years', 'yearly', 'annual', 'annually', 'yr', 'y']
    timescale_lst = day_lst + month_lst + year_lst

    # if timescale is None, then defaults to "day"
    if timescale is None: 
        timescale = "day"
        
    # if parameter is NOT in list of valid parameters
    if timescale not in timescale_lst:
        raise ValueError(f"Invalid `timescale` argument: '{timescale}'\nPlease enter one of the following valid timescales: \n{day_lst}\n{month_lst}\n{year_lst}")

    # request daily surface water time series data
    if timescale in day_lst:    
        sw_df = _get_sw_ts_day(
            abbrev              = abbrev,
            station_number      = station_number,
            usgs_id             = usgs_id,
            start_date          = start_date,
            end_date            = end_date,
            api_key             = api_key
            )

        # return daily surface water time series data
        return sw_df

    # request monthly surface water time series data
    if timescale in month_lst:    

        sw_df = _get_sw_ts_month(
            abbrev              = abbrev,
            station_number      = station_number,
            usgs_id             = usgs_id,
            start_date          = start_date,
            end_date            = end_date,
            api_key             = api_key
            )

        # return monthly surface water time series data
        return sw_df

    # request yearly surface water time series data
    if timescale in year_lst:    

        sw_df = _get_sw_ts_wyear(
            abbrev              = abbrev,
            station_number      = station_number,
            usgs_id             = usgs_id,
            start_date          = start_date,
            end_date            = end_date,
            api_key             = api_key
            )

        # return yearly surface water time series data
        return sw_df

def get_telemetry_stations(
    aoi            = None,
    radius         = None,
    abbrev         = None,
    county         = None,
    division       = None,
    gnis_id        = None,
    usgs_id        = None,
    water_district = None,
    wdid           = None,
    api_key        = None
    ):
    """Return Telemetry Station info

    Make a request to the /telemetrystations/telemetrystation endpoint to locate telemetry stations via a spatial search, or by station abbreviation, county, division, GNIS ID, USGS ID, water_district or WDID.  

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        abbrev (str, tuple, list, optional): Abbreviation name (or list of abbreviations) of the telemetry station. Defaults to None.
        county (str, optional): County to query for telemetry stations. Defaults to None.
        division (int, str, optional):  Water division to query for telemetry stations. Defaults to None.
        gnis_id (str, optional): GNIS ID of the telemetry station. Defaults to None.
        usgs_id (str, optional): USGS ID of the telemetry station. Defaults to None.
        water_district (int, str, optional): Water district to query for telemetry stations. Defaults to None.
        wdid (str, optional): WDID of the telemetry station. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.


    Returns:
        pandas dataframe object: dataframe of telemetry station data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, abbrev, county, division, gnis_id, usgs_id, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'abbrev', 'county', 'division', 'gnis_id', 'usgs_id', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    # base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrystation/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse site_id list, tuple, vector of site_id into query formatted string
    abbrev = _collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving telemetry station data")
    
    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&abbrev={abbrev or ""}'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&gnisId={gnis_id or ""}'
            f'&includeThirdParty=true'
            f'&usgsStationId={usgs_id or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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
    
    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )

    return data_df

def get_telemetry_ts(
    abbrev              = None,
    parameter           = "DISCHRG",
    start_date          = None,
    end_date            = None,
    timescale           = None,
    include_third_party = True,
    api_key             = None
    ):
    """Return Telemetry station time series data

    Make a request to the /telemetrystations/telemetrytimeseries endpoint to retrieve raw, hourly, or daily telemetry station time series data by station abbreviations, within a given date range (start and end dates).

    Args:
        abbrev (str, optional): Station abbreviation. Defaults to None.
        parameter (str, optional): Indicating which telemetry station parameter should be retrieved. Default is "DISCHRG" (discharge), all parameters are not available at all telemetry stations.. Defaults to "DISCHRG".
        start_date (str, optional): Date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): Date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): Data timescale to return, either "raw", "hour", or "day". Defaults to None and will request daily time series.
        include_third_party (bool, optional): Boolean, indicating whether to retrieve data from other third party sources if necessary. Defaults to True.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of telemetry station time series data
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "parameter", "start_date", "end_date", "timescale"],
        f        = any
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    # lists of valid timesteps
    timescale_lst = ["day", "hour", "raw"]

    # if timescale is None, then defaults to "day"
    if timescale is None: 
        timescale = "day"
        
    # if parameter is NOT in list of valid parameters
    if timescale not in timescale_lst:
        raise ValueError(f"Invalid `timescale` argument: '{timescale}'\nPlease enter one of the following valid timescales: \n{timescale_lst}")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrytimeseries" + timescale + "/?"

    # parse start_date into query string format
    start_date = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = _parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )
    
    # Create True or False include 3rd party string
    third_party_str = str(include_third_party).lower()

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print(f"Retrieving telemetry station time series data ({timescale} - {parameter})")


    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:
        
        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&abbrev={abbrev or ""}'
            f'&endDate={end_date or ""}'
            f'&startDate={start_date or ""}'
            f'&includeThirdParty={third_party_str or ""}'
            f'&parameter={parameter or ""}'
            f'&pageSize={page_size}&pageIndex={page_index}'
            )
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

        # extract dataframe from list column
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 
        
        # convert measDateTime and measDate columns to 'date' and pd datetime type
        if timescale == "raw":
            # convert measDate column to datetime column
            cdss_df['measDateTime'] = pd.to_datetime(cdss_df['measDateTime'])

        else: 
            # convert measDate column to datetime column
            cdss_df['measDate'] = pd.to_datetime(cdss_df['measDate'])

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df

def get_water_rights_netamount(
    aoi                 = None,
    radius              = None, 
    county              = None,
    division            = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):
    """Return water rights net amounts data

    Make a request to the /waterrights/netamount endpoint to retrieve water rights net amounts data via a spatial search or by county, division, water district, or WDID, within a given date range (start and end dates).
    Returns current status of a water right based on all of its court decreed actions.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        wdid (str, optional): WDID code of water right. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights net amounts data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving water rights net amounts data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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

    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )

    return data_df

def get_water_rights_trans(
    aoi                 = None,
    radius              = None, 
    county              = None,
    division            = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):
    """Return water rights transactions data

    Make a request to the /waterrights/transaction endpoint to retrieve water rights transactions data via a spatial search or by county, division, water district, or WDID, within a given date range (start and end dates).
    Returns List of court decreed actions that affect amount and use(s) that can be used by each water right.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights transactions. Defaults to None.
        water_district (str, optional): Water district to query for water rights transactions. Defaults to None.
        wdid (str, optional): WDID code of water right transaction. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights transactions data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/transaction/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = _check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving water rights transactions data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = _parse_gets(
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

    # mask data if necessary
    data_df = _aoi_mask(
        aoi = aoi,
        pts = data_df
        )
    
    return data_df

def get_call_analysis_wdid(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
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
    arg_lst = _check_args(
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
    start = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
        )

    # parse end_date into query string format
    end = _parse_date(
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

    print("Retrieving call analysis data by WDID")

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
        cdss_req = _parse_gets(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of call services by GNIS ID
    """
    
    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = _check_args(
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
    start = _parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
        )

    # parse end_date into query string format
    end = _parse_date(
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

    print("Retrieving call analysis data by GNIS ID")

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
        cdss_req = _parse_gets(
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
    arg_lst = _check_args(
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
        cdss_req = _parse_gets(
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
    arg_lst = _check_args(
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
        cdss_req = _parse_gets(
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

def _check_args(
        arg_dict = None, 
        ignore   = None,
        f        = any
        ):
    """Check all arguments of a function for any/all NULL values
    
    Internal function for checking a function arguments for any/all invalid/missing arguments necessary to the function it is called within
    
    Args:
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
        f (built-in function): Built in function "any" or "all" to indicate whether to check for "any" or "all" None argument. 
            If "any" then if any of the function arguments are None, then an error is thrown.
            If "all" then all relevant arguments must be None for an error to be thrown. Defaults to any.

    Returns:
        string: error statement with any/all None arguments listed, or None if no error is thrown by None values
    """

    # if no function arguments are given, throw an error
    if arg_dict is None:
        raise Exception("provide a list of function arguments by calling 'locals()', within another function")

    # argument dictionary key/values as lists
    key_lst  = list(arg_dict.keys())
    val_lst  = list(arg_dict.values())

    # if certain arguments are specifically supposed to be ignored
    if ignore is not None:
        
        # remove specifically ignorged arguments
        ignored_lst = [i for i, x in enumerate(key_lst) if x not in ignore]

        # keys and values of arguments to keep
        key_args        = [key_lst[i] for i in ignored_lst]
        val_args        = [val_lst[i] for i in ignored_lst]
    else:
        
        # if no arguments are ignored, keep all argument key/values
        key_args        = key_lst
        val_args        = val_lst

    # if any/all arguments are None, return an error statement. Otherwise return None if None check is passed
    if(f(i is None for i in val_args)):
        # check where in remaining arguments the value is None, and get the index of missing arguments
        idx_miss = [i for i in range(len(val_args)) if val_args[i] == None]

        # return the argument names of None arguments
        key_miss = ", ".join(["'"+key_lst[i]+"'" for i in idx_miss])

        # error print statement
        err_msg = "Invalid or missing " + key_miss + " arguments"

        return err_msg
    else:
        return None
    
def _parse_date(
    date   = None,
    start  = True,
    format =  "%m-%d-%Y"
    ):

    # if the date is the starting date
    if start == True:

        # if no start_date is given, default to 1900-01-01
        if date is None:
            date = "1900-01-01"
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
            date = date.strftime(format)
            date = date.replace("-", "%2F")
        else:
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
            date = date.strftime(format)
            date = date.replace("-", "%2F") 

    # if date is the ending date
    else:

        # if no end date is given, default to current date
        if date is None: 
            date   = datetime.date.today()
            date   = date.strftime(format)
            date   = date.replace("-", "%2F")
        else:
            date   = datetime.datetime.strptime(date, '%Y-%m-%d')
            date   = date.strftime(format)
            date   = date.replace("-", "%2F")

    return date

def _collapse_vector(
    vect = None, 
    sep  = "%2C+"
    ):
    
    # if a list of vects, collapse list
    if type(vect) == list or type(vect) == tuple:
        vect = [str(x) for x in vect]
        # join list into single string seperated by 'sep'
        vect = sep.join(vect)
        
        # replace white space w/ 'sep'
        vect = vect.replace(" ", sep)
    else:
        # if vect is an int or float, convert to string
        if type(vect) == int or type(vect) == float:
            vect = str(vect)
        
        if type(vect) == str:
            # replace white space w/ plus sign
            vect = vect.replace(" ", sep)
    
    return vect

def _get_error_handler(
    url      = None
    ):

    """ Make GET requests and return the responses

    Internal function for making get request and returning unsuccesful response text. 
    Used within a try, except block within _parse_gets() function.

    Args:
        url (str, optional): URL of the request
    
    Returns:
        requests.models.Response: returns results of attempted get request   
    """

    # If NO url is given
    if(url is None):
        raise Exception('Please provide a URL to perform a get request')
    
    # make API call

    # attempt GET request
    req_attempt = requests.get(url)

    # if request is 200 (OK), return JSON content data
    if req_attempt.status_code == 200:
        # return successful response
        return req_attempt
    else:
        # return req_attempt.text
        raise Exception(req_attempt.text)
    

def _parse_gets(
        url      = None, 
        arg_dict = None, 
        ignore   = None
        ):
    
    """ Makes GET requests and dynamically handle errors 

    Internal function for handling GET requests and associated errors.
    Function will try to make a GET request, and if an error occurs, the function 
    will return an error message with relevenant information detailing the error 
    and the inputs that led to the error.

    Args:
        url (str): URL of the request
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
    
    Returns:
        requests response: returns results of attempted get request 
    """

    # try to make GET request and error handling unsuccessful requests
    try:
        # attempt GET request
        req = _get_error_handler(url = url)

        return(req)
    
    except Exception as e:

        # if an error occurred, use _query_error() to format a helpful error message to user
        raise Exception(_query_error(
            arg_dict = arg_dict,
            url      = url,
            ignore   = ignore,
            e_msg    = e
            )
            )
    
def _query_error(
        arg_dict = None,
        url      = None,
        ignore   = None,
        e_msg    = None
        ):
    """GET Request Error message handler

    Internal function for generating dynamic error messages for failed GET requests.
    Designed to be called within another function and print out the functions input arguments.

    Args:
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        url (str): URL of the request. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
        e_msg (exception, str): exception message or string message that should be pointed to as the original error message. Defaults to None.

    Returns:
        str: error message that includes the query inputs that led to the error, the requested URL, and the original error message
    """

    # if no function arguments are given, throw an error
    if arg_dict is None:
        raise Exception("provide a list of function arguments by calling 'locals()', within another function")
    
    # argument dictionary key/values as lists
    key_lst  = list(arg_dict.keys())
    val_lst  = list(arg_dict.values())

    # if certain arguments are specifically supposed to be ignored
    if ignore is not None:
        
        # remove specifically ignorged arguments
        ignored_lst = [i for i, x in enumerate(key_lst) if x not in ignore]

        # keys and values of arguments to keep
        key_args        = [key_lst[i] for i in ignored_lst]
        val_args        = [val_lst[i] for i in ignored_lst]
    else:
        
        # if no arguments are ignored, keep all argument key/values
        key_args        = key_lst
        val_args        = val_lst

    # query_dict = dict(zip(key_args, val_args))

    q_lst = []

    for i in range(len(key_args)):
            q_lst.append(f'{key_args[i]}: {val_args[i]}')

    q_msg = ("DATA RETRIEVAL ERROR\nQuery:\n" + '\n'.join(q_lst) +
            "\nRequested URL: " + url + 
            "\n\n" + "Original error message: " + "\n-----------------------\n\n" + str(e_msg)
            )

    return q_msg

def _get_error_handler2(
    url     = None
    ):

    """
    Internal function for making get request and error handling unsuccessful requests

    Args:
        url (str, optional): URL of the request
    
    Returns:
        requests.models.Response: returns results of attempted get request   
    """

    # If NO url is given
    if(url is None):
        raise Exception('Please provide a URL to perform a get request')
    
    # make API call
    try:
        # attempt GET request
        req_attempt = requests.get(url)

        req_attempt.raise_for_status()

        # return successful response
        return req_attempt

    except requests.exceptions.HTTPError as errh:
        print("HTTP Error:\n", errh)
        print("\nClient response:\n", errh.response.text)
        raise
    except requests.exceptions.ConnectionError as errc:
        print("Connection Error:\n", errc)
        print("\nClient response:\n", errc.response.text)
        raise
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:\n", errt)
        print("\nClient response:\n", errt.response.text)
        raise
    except requests.exceptions.RequestException as err:
        print("Exception raised:\n", err)
        print("\nClient response:\n", err.response.text)
        raise

def _aoi_error_msg():
    """
    Function to return error message to user when aoi is not valid.
    Returns:
        string: print statement for aoi errors
    """

    msg = ("\nInvalid 'aoi' argument, 'aoi' must be one of the following:\n" + 
    "1. List/Tuple of an XY coordinate pair\n" +
    "2. Dictionary with X and Y keys\n" +
    "3. Pandas DataFrame containing XY coordinates\n" +
    "4. a shapely Point/Polygon/LineString\n" +
    "5. Geopandas GeoDataFrame containing a Polygon/LineString/LinearRing/Point geometry\n" +
    "6. Geopandas GeoSeries containing a Polygon/LineString/Point geometry\n")

    return msg

def _check_coord_crs(epsg_code, lng, lat):

    """Function that checks if a set of longitude and latitude points are within a given EPSG space.

    Returns:
        boolean: True if the coordinates are within the provided EPSG space, False otherwise.
    """
    # given crs epsg code
    crs = pyproj.CRS.from_user_input(epsg_code)

    # if lng/lat fall within CRS space
    if((crs.area_of_use.south <= lat <= crs.area_of_use.north) and (crs.area_of_use.west <= lng <= crs.area_of_use.east)):
        crs_check = True
    else:
        crs_check = False

    return crs_check

def _extract_shapely_coords(aoi):
    """Function for extracting coordinates from a shapely Polygon/LineString/Point
    Internal helper function used in location search queries.

    Args:
        aoi (shapely Polygon/LineString/Point): shapely Polygon/LineString/Point object to extract coordinates from

    Returns:
        list: list of string coordinates with precision of 5 decimal places
    """

    # ensure that the shapely geometry is either a Polygon, LineString or a Point
    if(isinstance(aoi, (shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString, shapely.geometry.point.Point))):

        # if geometry is Polygon or Linestring
        if(isinstance(aoi, (shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString))):
            # extract lng/lat coords
            lng = aoi.centroid.x
            lat = aoi.centroid.y

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # Valid coords in correct CRS space
            if(_check_coord_crs(epsg_code = 4326, lng = lng, lat = lat)):

                # round coordinates to 5 decimal places
                coord_lst = [f'{num:.5f}' for num in coord_lst]

                # return list of coordinates
                return coord_lst

            else:
                raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")

        # if geometry is a Point
        if(isinstance(aoi, (shapely.geometry.point.Point))):
            # extract lng/lat coords
            lng = aoi.x
            lat = aoi.y
            
            # lng, lat coordinates
            coord_lst = [lng, lat]

            # Valid coords in correct CRS space
            if(_check_coord_crs(epsg_code = 4326, lng = lng, lat = lat)):

                # round coordinates to 5 decimal places
                coord_lst = [f'{num:.5f}' for num in coord_lst]

                # return list of coordinates
                return coord_lst

            else:
                raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")
    else:
        raise Exception("Invalid 'aoi' shapely geometry, must be either a shapely Polygon, LineString or Point")



def _extract_coords(
    aoi = None
    ):

    """Internal function for extracting XY coordinates from aoi arguments
    Function takes in a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries of spatial objects,
    and returns a list of length 2, indicating the XY coordinate pair. 
    If the object provided is a Polygon/LineString/LinearRing, the function will return the XY coordinates of the centroid of the spatial object.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
    
    Returns:
        list object: list object of an XY coordinate pair
    """
    # if None is passed to 'aoi', return None
    if aoi is None: 

        return None

    # if 'aoi' is NOT none, extract XY coordinates from object
    else:

        # make sure 'aoi' is one of supported types
        if(isinstance(aoi, (list, tuple, dict, geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame, 
        pd.core.frame.DataFrame, shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString, shapely.geometry.point.Point)) is False):
            raise Exception(_aoi_error_msg())

        # check if aoi is a list or tuple
        if(isinstance(aoi, (list, tuple))):

            if(len(aoi) >= 2):

                # make coordinate list of XY values
                coord_lst = [aoi[0], aoi[1]]

                # round coordinates to 5 decimal places
                coord_lst = [float(num) for num in coord_lst]

                # Valid coords in correct CRS space
                if(_check_coord_crs(epsg_code = 4326, lng = coord_lst[0], lat = coord_lst[1])):

                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

                else:
                    raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")

            else:

                # return list of coordinates
                raise Exception(_aoi_error_msg())

        # check if aoi is a shapely Polygon/LineString/Point
        if("shapely" in str(type(aoi))):

            # extract coordinates from shapely geometry objects
            coord_lst = _extract_shapely_coords(aoi = aoi)

            return coord_lst 

        # check if aoi is a geopandas geoseries or geodataframe 
        if(isinstance(aoi, (geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame))):
            
            if(len(aoi) > 1):
                raise Exception(_aoi_error_msg())

            # convert CRS to 5070
            aoi = aoi.to_crs(5070)

            # if aoi geometry type is polygon/line/linearRing
            if(["Polygon", 'LineString', 'LinearRing'] in aoi.geom_type.values):

                # checking if point is geopandas Geoseries
                if(isinstance(aoi, (geopandas.geoseries.GeoSeries))):

                    # get centroid of polygon, and convert to 4326 and add lng/lat as column
                    lng = float(aoi.centroid.to_crs(4326).geometry.x)
                    lat = float(aoi.centroid.to_crs(4326).geometry.y)

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

                # checking if point is geopandas GeoDataFrame
                if(isinstance(aoi, (geopandas.geodataframe.GeoDataFrame))):

                    # get centroid of polygon, and convert to 4326 and add lng/lat as column
                    aoi["lng"] = aoi.centroid.to_crs(4326).map(lambda p: p.x)
                    aoi["lat"] = aoi.centroid.to_crs(4326).map(lambda p: p.y)

                    # subset just lng/lat cols
                    aoi_coords = aoi.loc[ : , ['lng', 'lat']]

                    # extract lat/lng from centroid of polygon
                    lng = float(aoi_coords["lng"])
                    lat = float(aoi_coords["lat"])

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

            # if aoi geometry type is point
            if("Point" in aoi.geom_type.values):

                # checking if point is geopandas Geoseries
                if(isinstance(aoi, (geopandas.geoseries.GeoSeries))):

                    # convert to 4326, and extract lat/lng from Pandas GeoSeries
                    lng = float(aoi.to_crs(4326).x)
                    lat = float(aoi.to_crs(4326).y)

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]
                    
                    # return list of coordinates
                    return coord_lst

                # checking if point is geopandas GeoDataFrame
                if(isinstance(aoi, (geopandas.geodataframe.GeoDataFrame))):

                    # convert to 4326, and extract lat/lng from Pandas GeoDataFrame
                    lng = float(aoi.to_crs(4326)['geometry'].x)
                    lat = float(aoi.to_crs(4326)['geometry'].y)
                
                    # lng, lat coordinates
                    coord_lst = [lng, lat]

                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst
                    
        # check if aoi is a Pandas dataframe
        if(isinstance(aoi, (pd.core.frame.DataFrame))):
            
            # extract first and second columns
            lng = float(aoi.iloc[:, 0])
            lat = float(aoi.iloc[:, 1])

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # round coordinates to 5 decimal places
            coord_lst = [f'{num:.5f}' for num in coord_lst]

            # return list of coordinates
            return coord_lst

        # check if aoi is a dictionary
        if(isinstance(aoi, (dict))):
            
            # extract "X" and "Y" dict keys
            lng = float(aoi["X"])
            lat = float(aoi["Y"])

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # round coordinates to 5 decimal places
            coord_lst = [f'{num:.5f}' for num in coord_lst]

            # return list of coordinates
            return coord_lst

def _check_radius(
    aoi    = None,
    radius = None
    ):

    """Internal function for radius argument value is within the valid value range for location search queries. 

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None.

    Returns:
        int: radius value between 1-150 miles
    """
    # convert str radius value to int
    if(isinstance(radius, (str))):
        radius = int(radius)

    # if spatial data is provided, check type and try to extract XY coordinates
    if aoi is not None:
        # print("AOI arg is NOT None")

        # if radius is not NULL, and is larger than 150, set to max of 150. if NULL radius is provided with spatial data, default to 150 miles
        if radius is not None:
            # print("radius arg is NOT None")
            
            # if radius value is over max, set to 150
            if(radius > 150):

                # print("radius arg > 150, so set to 150")
                radius = 150

            # if radius value is under min, set to 1
            if(radius <= 0):
                # print("radius arg <= 0, so set to 1")
                radius = 1

        # if no radius given, set to 20 miles
        else:
            # print("radius arg is None, default to 20 miles")
            radius = 20
    else:
        # print("AOI arg is None")
        radius = None
    
    # Return radius value
    return radius


def _check_aoi(
    aoi    = None, 
    radius = None
    ):

    """Internal function for checking AOI and radius arguments are valid for use in location search queries.
    Function takes in a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries of spatial objects,
    along with a radius value between 1-150 miles.
    The extracts the necessary coordinates from the given aoi parameter and also makes sure the radius value is within the valid value range. 
    The function then returns a list of length 2, indicating the XY coordinate pair. 
    If the object provided is a Polygon/LineString/LinearRing, the function will return the XY coordinates of the centroid of the spatial object.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None.

    Returns:
        list: list containing the latitude, longitude, and radius values to use for location search queries.
    """

    # convert str radius value to int
    if(isinstance(radius, (str))):
        radius = int(radius)

    # extract lat/long coords for query
    if(aoi is not None):
        # extract coordinates from matrix/dataframe/sf object
        coord_df = _extract_coords(aoi = aoi)
        
        # check radius is valid and fix if necessary
        radius = _check_radius(
            aoi    = aoi,
            radius = radius
            )

        # lat/long coords
        lng = coord_df[0]
        lat = coord_df[1]
    
    else:
        # if None aoi given, set coords and radius to None
        lng    = None
        lat    = None
        radius = None
    
    # create list to return container longitude, latitude, and radius
    aoi_lst = [lng, lat, radius]
    
    # return lng, lat, radius list
    return aoi_lst  

def _aoi_mask(
    aoi = None,
    pts = None
    ):

    """For location search queries using a polygon, the response data from the CDSS API will be masked to the polygon area, removing any extra points.
    Internal helper function, if aoi is None, then the function will just return the original dataset. 

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        pts (pandas dataframe): pandas dataframe of points that should be masked to the given aoi. Dataframe must contain "utmY" and "utmX" columns

    Returns:
        pandas dataframe: pandas dataframe with all points within the given aoi polygon area
    """

    # if AOI and pts are None, return None
    if all(i is None for i in [aoi, pts]):
        return None

    # if no 'aoi' is given (None), just return original pts data. Default behavior
    if(aoi is None):
        return pts
    
    # check if aoi is a shapely geometry polygon
    if(isinstance(aoi, (shapely.geometry.polygon.Polygon))):

        # if aoi geometry type is polygon/line/linearRing
        if("Polygon" in aoi.geom_type):

            rel_pts = geopandas.overlay(
                geopandas.GeoDataFrame(pts, geometry = geopandas.points_from_xy(pts['utmX'], pts['utmY'])).set_crs(26913).to_crs(4326), 
                geopandas.GeoDataFrame(index = [0], crs = 'epsg:4326', geometry = [aoi]), 
                how = 'intersection'
                )

            # convert geopandas dataframe to pandas dataframe and drop geometry column
            rel_pts = pd.DataFrame(rel_pts.drop(columns='geometry'))

            return rel_pts
        else:
            return pts
    
        
    # check if aoi is a geopandas geoseries or geodataframe 
    if(isinstance(aoi, (geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame))):

        # if aoi geometry type is polygon/line/linearRing
        if(["Polygon"] in aoi.geom_type.values):

            # convert CRS to 4326
            aoi = aoi.to_crs(4326)
            
            # get intersection of points and polygons 
            rel_pts = geopandas.overlay(
                geopandas.GeoDataFrame(pts, geometry = geopandas.points_from_xy(pts['utmX'], pts['utmY'])).set_crs(26913).to_crs(4326), 
                geopandas.GeoDataFrame(aoi.geometry),
                how = 'intersection'
                )

            # convert geopandas dataframe to pandas dataframe and drop geometry column
            rel_pts = pd.DataFrame(rel_pts.drop(columns='geometry'))

            return rel_pts
        else:
            return pts
    else:
        return pts