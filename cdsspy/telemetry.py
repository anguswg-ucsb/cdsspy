import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

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
    arg_lst = utils._check_args(
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
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse site_id list, tuple, vector of site_id into query formatted string
    abbrev = utils._collapse_vector(
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
        parameter (str, optional): Indicating which telemetry station parameter should be retrieved. Default is "DISCHRG" (discharge), all parameters are not available at all telemetry stations. Defaults to "DISCHRG".
        start_date (str, optional): Date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): Date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): Data timescale to return, either "raw", "hour", or "day". Defaults to None and will request daily time series.
        include_third_party (bool, optional): Boolean, indicating whether to retrieve data from other third party sources if necessary. Defaults to True.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of telemetry station time series data
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
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
        cdss_req = utils._parse_gets(
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