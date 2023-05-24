import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
from cdsspy import utils

def _get_structures_divrecday(
    wdid          = None,
    wc_identifier = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Daily Diversion/Release Records

    Make a request to the api/v2/structures/divrec/divrecday/ endpoint to retrieve daily structure diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  series of water class codes that provide the location of the diversion, the SOURCE of water, the USE of the water and the administrative operation required to make the diversion. Provide "diversion" or "release" to retrieve diversion/release records. Default is None which will return diversions records.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure diversion/releases records 
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecday/?"

    # correctly format wc_identifier, if NULL, return "*diversion*"
    wc_id = utils._align_wcid(
        x       = wc_identifier,
        default = "*diversion*"
        )
    
    # collapse list, tuple, vector of wdid into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
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
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    # print message
    if wc_identifier is None: 
        print(f'Retrieving daily divrec data (diversion)')
    else:
        print(f'Retrieving daily divrec data ({wc_identifier})')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier={wc_id or ""}'
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


def _get_structures_divrecmonth(
    wdid          = None,
    wc_identifier = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Monthly Diversion/Release Records

    Make a request to the api/v2/structures/divrec/divrecmonth/ endpoint to retrieve monthly structure diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  series of water class codes that provide the location of the diversion, the SOURCE of water, the USE of the water and the administrative operation required to make the diversion. Provide "diversion" or "release" to retrieve diversion/release records. Default is None which will return diversions records.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of monthly structure diversion/releases records 
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecmonth/?"

    # correctly format wc_identifier, if NULL, return "*diversion*"
    wc_id = utils._align_wcid(
        x       = wc_identifier,
        default = "*diversion*"
        )
    
    # collapse list, tuple, vector of wdid into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = utils._parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%Y"
    )

    # parse end_date into query string format
    end_date = utils._parse_date(
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

    # print message
    if wc_identifier is None: 
        print(f'Retrieving monthly divrec data (diversion)')
    else:
        print(f'Retrieving monthly divrec data ({wc_identifier})')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier={wc_id or ""}'
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

def _get_structures_divrecyear(
    wdid          = None,
    wc_identifier = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return Structure Annual Diversion/Release Records

    Make a request to the structures/divrec/divrecyear/ endpoint to retrieve annual structure diversion/release data for a specified WDID within a specified date range.

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  series of water class codes that provide the location of the diversion, the SOURCE of water, the USE of the water and the administrative operation required to make the diversion. Provide "diversion" or "release" to retrieve diversion/release records. Default is None which will return diversions records.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of annual structure diversion/releases records 
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "wc_identifier", "start_date", "end_date"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecyear/?"

    # correctly format wc_identifier, if NULL, return "*diversion*"
    wc_id = utils._align_wcid(
        x       = wc_identifier,
        default = "*diversion*"
        )

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
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
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    # print message
    if wc_identifier is None: 
        print(f'Retrieving yearly divrec data (diversion)')
    else:
        print(f'Retrieving yearly divrec data ({wc_identifier})')

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&wcIdentifier={wc_id or ""}'
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

def get_structures_divrec_ts(
    wdid          = None,
    wc_identifier = None,
    start_date    = None,
    end_date      = None,
    timescale     = None, 
    api_key       = None
    ):

    """Return diversion/releases record data for administrative structures

    Make a request to the CDSS API /structures/divrec endpoints to get diversion/releases time series data for administrative structures by wdid, within a given date range (start and end dates) and at a specified temporal resolution.     

    Args:
        wdid (str, optional):  tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional):  series of water class codes that provide the location of the diversion, the SOURCE of water, the USE of the water and the administrative operation required to make the diversion. Provide "diversion" or "release" to retrieve diversion/release records. Default is None which will return diversions records.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): timestep of the time series data to return, either "day", "month", or "year". Defaults to None and will request daily time series.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of structure diversion/releases time series data
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
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
        divrec_df = _get_structures_divrecday(
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

        divrec_df = _get_structures_divrecmonth(
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

        divrec_df = _get_structures_divrecyear(
            wdid          = wdid,
            wc_identifier = wc_identifier,
            start_date    = start_date,
            end_date      = end_date,
            api_key       = api_key
            )

        # return yearly structure divrec time series data
        return divrec_df

def get_structures_stage_ts(
    wdid          = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Return stage/volume record data for administrative structures

    Make a request to the structures/divrec/stagevolume/ endpoint to retrieve structure stage/volume data for a specified WDID within a specified date range.

    Args:
        wdid (str):  WDID code of structure. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):   optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure stage/volume records 
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
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/stagevolume/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
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
        wdid (str, tuple or list, optional): WDID(s) code of structure. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of administrative structures
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
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/?"

    # convert numeric division to string
    if type(division) == int or type(division) == float:
        division = str(division)

    # convert numeric water_district to string
    if type(water_district) == int or type(water_district) == float:
        water_district = str(water_district)

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse WDID list, tuple, vector of site_id into query formatted string
    wdid = utils._collapse_vector(
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

def get_water_classes(
        wdid                = None,
        county              = None,
        division            = None,
        water_district      = None,
        wc_identifier       = None,
        aoi                 = None,
        radius              = None,
        gnis_id             = None,
        start_date          = None,
        end_date            = None,
        divrectype          = None,
        ciu_code            = None,
        timestep            = None,
        api_key             = None
        ):
    """Return list of waterclasses

    Make a request to the /structures/divrec/waterclasses endpoint to identify water classes via a spatial search or by division, county, water_district, GNIS, or WDID.

    Args:
        wdid (str, tuple or list, optional): WDID(s) code of structure. Defaults to None.
        county (str, optional): county to query. Defaults to None.
        division (str, int, optional): water division to query. Defaults to None.
        water_district (str, int, optional): water district to query. Defaults to None.
        wc_identifier (_type_, optional): series of water class codes that provide the location of the diversion, the SOURCE of water, the USE of the water and the administrative operation required to make the diversion. The Water Class, combined with a daily, monthly or annual volume, constitutes a Diversion Record. Defaults to None.
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        gnis_id (str, optional): water source - Geographic Name Information System ID. Defaults to None.
        start_date (str, optional): date of first measurement in the well's period of record (YYYY-MM-DD). Defaults to None.
        end_date (str, optional): date of last measurement in the well's period of record (YYYY-MM-DD). Defaults to None.
        divrectype (str, optional): type of record: "DivComment", "DivTotal", "RelComment", "RelTolal", "StageVolume", or "WaterClass".. Defaults to None.
        ciu_code (str, optional): current in use code of structure. Defaults to None.
        timestep (str, optional): timestep, one of "day", "month", "year". Defaults to None which returns a daily timestep.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water class data for administrative structures
    """

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key", "start_date", "end_date", "aoi", "radius",
                    "ciu_code", "divrectype", "gnis_id", "timestep"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/waterclasses/?"

    # correctly format wc_identifier, if NULL, return "*diversion*"
    wc_id = utils._align_wcid(
        x       = wc_identifier,
        default = None
        )
    
    # collapse list, tuple, vector of wdid into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )
    
    # if start_date is None, return None
    if start_date is None:
        start = None
    else:
        # parse start_date into query string format
        start = utils._parse_date(
            date   = start_date,
            start  = True,
            format = "%m-%d-%Y",
            sep    = "%2F"
        )

    # if end_date is None, return None
    if end_date is None:
        end = None
    else:
        # parse start_date into query string format
        end = utils._parse_date(
            date   = end_date,
            start  = False,
            format = "%m-%d-%Y",
            sep    = "%2F"
        )

    # collapse WDID list, tuple, vector of site_id into query formatted string
    wdid = utils._collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )
    
    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # if county is given, make sure it is separated by "+" and all uppercase 
    if county is not None:
        county = county.replace(" ", "+")
        county = county.upper()
    
    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df   = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    # print message
    print("Retrieving structure water classes")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:
        
        # create query URL string
        url = (
            f'{base}'
            f'timestep={timestep or ""}'
            f'format=json&dateFormat=spaceSepToSeconds'
            f'&ciuCode={ciu_code or ""}'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&divrectype={divrectype or ""}'
            f'&min-porEnd={end or ""}'
            f'&min-porStart={start or ""}'
            f'&gnisId={gnis_id or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wcIdentifier={wc_id or ""}'
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

    return data_df