import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

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

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse abbrev list, tuple, vector of abbrev into query formatted string
    abbrev = utils._collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse usgs_id list, tuple, vector of usgs_id into query formatted string
    usgs_id = utils._collapse_vector(
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
    
    # mask data if necessary
    data_df = utils._aoi_mask(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
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
    arg_lst = utils._check_args(
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
    abbrev = utils._collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = utils._collapse_vector(
        vect = usgs_id, 
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
        cdss_req = utils._parse_gets(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
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
    arg_lst = utils._check_args(
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
    abbrev = utils._collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = utils._collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = utils._parse_date(
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
        cdss_req = utils._parse_gets(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
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
    arg_lst = utils._check_args(
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
    abbrev = utils._collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = utils._collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = utils._parse_date(
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
        cdss_req = utils._parse_gets(
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
    arg_lst = utils._check_args(
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