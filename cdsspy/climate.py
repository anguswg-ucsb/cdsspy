import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
# from .utils import shared_function
from cdsspy import utils

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

    # collapse site_id list, tuple, vector of site_id into query formatted string
    site_id = utils._collapse_vector(
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
        end_date (str, optional): date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station frost dates data
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
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationfrostdates/?"
    
    # parse start_date into query string format
    start_year = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_year = utils._parse_date(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
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
    arg_lst = utils._check_args(
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
    site_id = utils._collapse_vector(
        vect = site_id, 
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station monthly time series data
    """
    # list of valid parameters
    param_lst = ["Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow","SnowDepth", "SnowSWE", "Solar","VP", "Wind"]

    # if parameter is not in list of valid parameters
    if param not in param_lst:
        raise ValueError("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")
    
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
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationtsmonth/?"

    # collapse list, tuple, vector of site_id into query formatted string
    site_id = utils._collapse_vector(
        vect = site_id, 
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
        cdss_req = utils._parse_gets(
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
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): timestep of the time series data to return, either "day" or "month". Defaults to None and will request daily time series.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station time series data
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
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