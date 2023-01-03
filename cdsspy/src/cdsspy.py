# __init__.py
__version__ = "1.0.8"

import pandas as pd
import requests
import datetime
import geopandas

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
    Make a request to the api/v2/administrative calls endpoint to locate active or historical administrative calls by division, location WDID, or call number within a specified date range.

    Args:
        division (int, str, optional): Water division to query for administrative calls. Defaults to None.
        location_wdid (str, optional): call location structure WDID to query for administrative calls. Defaults to None.
        call_number (int, str, optional): unique call identifier to query. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        active (bool, optional): whether to get active or historical administrative calls. Defaults to True which returns active administrative calls.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of active/historical administrative calls data
    """
    # If all inputs are None, then return error message
    if all(i is None for i in [division, location_wdid, call_number]):
        raise TypeError("Invalid 'division', 'location_wdid', or 'call_number' parameters")
    
    # collapse location_wdid list, tuple, vector of site_id into query formatted string
    location_wdid = collapse_vector(
        vect = location_wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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
    Make a request to the climatedata/climatestations/ endpoint to locate climate stations by AOI, county, division, station name, Site ID or water_district.
    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for climate stations. Defaults to None.
        division (int, str, optional):  Water division to query for climate stations. Defaults to None.
        station_name (str, optional): string, climate station name. Defaults to None.
        site_id (str, tuple, list, optional): string, tuple or list of site IDs. Defaults to None.
        water_district (int, str, optional): Water district to query for climate stations. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.


    Returns:
        pandas dataframe object: dataframe of climate station data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [aoi, county, division, station_name, site_id, water_district]):
        raise TypeError("Invalid 'aoi', 'county', 'division', 'station_name', 'site_id', or 'water_district' parameters")

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse site_id list, tuple, vector of site_id into query formatted string
    site_id = collapse_vector(
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

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_climate_frostdates(
    station_number      = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return climate stations frost dates 

    Args:
        station_number (str, optional): climate data station number. Defaults to None.
        start_date (str, optional): date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station frost dates data
    """
    # If all inputs are None, then return error message
    if all(i is None for i in [station_number]):
        raise TypeError("Invalid 'station_number' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationfrostdates/?"
    
    # parse start_date into query string format
    start_year = parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_year = parse_date(
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

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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


def get_climate_ts_day(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return daily climate data

    Args:
        station_number (str, optional): string, climate data station number. Defaults to None.
        site_id (str, tuple, list, optional): string, tuple or list of climate station site IDs. Defaults to None.
        param (str): string, climate variable. One of: "Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow", "SnowDepth", "SnowSWE", "Solar","VP", "Wind". Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station daily timeseries data
    """

    # list of valid parameters
    param_lst = ["Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow","SnowDepth", "SnowSWE", "Solar","VP", "Wind"]

    # if parameter is not in list of valid parameters
    if param not in param_lst:
        raise ValueError("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # If all inputs are None, then return error message
    if all(i is None for i in [site_id, station_number]):
        raise TypeError("Invalid 'site_id' or 'station_number' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationtsday/?"

    # collapse list, tuple, vector of site_id into query formatted string
    site_id = collapse_vector(
        vect = site_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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
    
    print("Retrieving climate station daily timeseries data")

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_climate_ts_month(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return monthly climate data

    Args:
        station_number (str, optional): string, climate data station number. Defaults to None.
        site_id (str, optional): string, tuple or list of climate station site IDs. Defaults to None.
        param (str, optional): string, climate variable. One of: "Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow", "SnowDepth", "SnowSWE", "Solar","VP", "Wind". Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of climate station monthly timeseries data
    """
    # list of valid parameters
    param_lst = ["Evap", "FrostDate",  "MaxTemp", "MeanTemp", "MinTemp", "Precip", "Snow","SnowDepth", "SnowSWE", "Solar","VP", "Wind"]

    # if parameter is not in list of valid parameters
    if param not in param_lst:
        raise ValueError("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # If all inputs are None, then return error message
    if all(i is None for i in [site_id, station_number]):
        raise TypeError("Invalid 'site_id' or 'station_number' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/climatedata/climatestationtsmonth/?"

    # collapse list, tuple, vector of site_id into query formatted string
    site_id = collapse_vector(
        vect = site_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

    print("Retrieving climate station monthly timeseries data")

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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
    if all(i is None for i in [county, designated_basin, division, management_district, water_district, wellid]):
        raise TypeError("Invalid 'county', 'designated_basin', 'division', 'management_district', 'water_district', or 'wellid' parameters")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

    Args:
        wellid (str): Well ID to query for groundwater water level measurements. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater well measurements
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [wellid]):
        raise TypeError("Invalid 'wellid' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/groundwater/waterlevels/wellmeasurements/?"

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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
    if all(i is None for i in [county, designated_basin, division, management_district, water_district, wellid]):
        raise TypeError("Invalid 'county', 'designated_basin', 'division', 'management_district', 'water_district', or 'wellid' parameters")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

    Args:
        wellid (str, optional): Well ID of a groundwater geophysicallog wells. Defaults to None.
        api_key (str, optional):  API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.

    Returns:
        pandas dataframe object: dataframe of groundwater geophysical log picks
    """
    # If all inputs are None, then return error message
    if all(i is None for i in [wellid]):
        raise TypeError("Invalid 'wellid' parameter")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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
    
    Args:
        table_namDee (str, optional): Name of the reference table to return. One of: "county", "waterdistricts", "waterdivisions", "designatedbasins", "managementdistricts", "telemetryparams", "climateparams", "divrectypes", "flags". Defaults to none.
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
        ref_table = get_ref_county(
            api_key = api_key
            )
        return ref_table
    
    # retrieve water districts reference table
    if table_name == "waterdistricts":
        ref_table = get_ref_waterdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve water divisions reference table
    if table_name == "waterdivisions":
        ref_table = get_ref_waterdivisions(
            api_key = api_key
            )
        return ref_table

    # retrieve management districts reference table
    if table_name == "managementdistricts":
        ref_table = get_ref_managementdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve designated basins reference table
    if table_name == "designatedbasins":
        ref_table = get_ref_designatedbasins(
            api_key = api_key
            )
        return ref_table

    # retrieve telemetry station parameters reference table
    if table_name == "telemetryparams":
        ref_table = get_ref_telemetry_params(
            api_key = api_key
            )
        return ref_table

    # retrieve climate station parameters reference table
    if table_name == "climateparams":
        ref_table = get_ref_climate_params(
            api_key = api_key
            )
        return ref_table

    # retrieve diversion record types reference table
    if table_name == "divrectypes":
        ref_table = get_ref_divrectypes(
            api_key = api_key
            )
        return ref_table

    # retrieve station flags reference table
    if table_name == "flags":
        ref_table = get_ref_stationflags(
            api_key = api_key
            )
        return ref_table
        
def get_ref_county(
    county  = None, 
    api_key = None
    ):
    """Return county reference table

    Args:
        county (str, optional): string, (optional) indicating the county to query, if no county is given, entire county dataframe is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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


def get_ref_waterdistricts(
    division       = None, 
    water_district = None,
    api_key        = None
    ):
    """Return water districts reference table

    Args:
        division (str, optional): string, (optional) indicating the division to query, if no division is given, dataframe of all water districts is returned. Defaults to None.
        water_district (str, optional): string, (optional) indicating the water district to query, if no water district is given, dataframe of all water districts is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_waterdivisions(
    division       = None, 
    api_key        = None
    ):
    """Return water divisions reference table

    Args:
        division (str, optional): Division to query, if no division is given, dataframe of all water divisions is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_managementdistricts(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_designatedbasins(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_telemetry_params(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_climate_params(
    param   = None, 
    api_key            = None
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_divrectypes(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_ref_stationflags(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_structure_divrecday(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Request Structure Daily Diversion/Release Records

    Args:
        wdid (str, optional): string, tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional): string, indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  string, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure diversion/releases records 
    """

    # if no wdid is given, return error
    if wdid is None:
        raise TypeError("Invalid 'wdid' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecday/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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
        # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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


def get_structure_divrecmonth(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Request Structure Monthly Diversion/Release Records

    Args:
        wdid (str, optional): string, tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional): string, indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  string, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of monthly structure diversion/releases records 
    """
    # if no wdid is given, return error
    if wdid is None:
        raise TypeError("Invalid 'wdid' parameter")
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecmonth/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_structure_divrecyear(
    wdid          = None,
    wc_identifier = "diversion",
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Request Structure Annual Diversion/Release Records

    Args:
        wdid (str, optional): string, tuple or list of WDIDs code of structure. Defaults to None.
        wc_identifier (str, optional): string, indicating whether "diversion" or "release" should be returned. Defaults to "diversion".
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  string, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of annual structure diversion/releases records 
    """

    # if no wdid is given, return error
    if wdid is None:
        raise TypeError("Invalid 'wdid' parameter")
    
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/divrecyear/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_structure_stage(
    wdid          = None,
    start_date    = None,
    end_date      = None,
    api_key       = None
    ):
    """Request Structure stage/volume Records

    Args:
        wdid (str, optional): string, tuple or list of WDIDs code of structure. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional):  string, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of daily structure stage/volume records 
    """

    # if no abbreviation is given, return error
    if wdid is None:
        raise TypeError("Invalid 'wdid' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/divrec/stagevolume/?"

    # collapse list, tuple, vector of wdid into query formatted string
    wdid = collapse_vector(
        vect = wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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
    
    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): Indicating the county to query. Defaults to None.
        division (int, str, optional): Indicating the water division to query. Defaults to None.
        gnis_id (str, optional): Water source - Geographic Name Information System ID (GNIS ID). Defaults to None.
        water_district (int, str, optional): Indicating the water district to query. Defaults to None.
        wdid (string, tuple or list, optional): WDIDs code of structure. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of administrative structures
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [aoi, county, division, gnis_id, water_district, wdid]):
        raise TypeError("Invalid 'aoi', 'county', 'division', 'gnis_id', 'water_district', or 'wdid' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/?"

    # convert numeric division to string
    if type(division) == int or type(division) == float:
        division = str(division)

    # convert numeric water_district to string
    if type(water_district) == int or type(water_district) == float:
        water_district = str(water_district)

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse WDID list, tuple, vector of site_id into query formatted string
    wdid = collapse_vector(
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

    # Loop through pages until last page of data is found, binding each responce dataframe together
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
        
        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_sw_ts_day(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Request daily surface water timeseries data

    Args:station_number (str, optional): string, climate data station number. Defaults to None.
        abbrev (_type_, optional): string, tuple or list of surface water station abbreviation. Defaults to None.
        station_number (_type_, optional): string, surface water station number. Defaults to None.
        usgs_id (_type_, optional): string, tuple or list of USGS ID. Defaults to None.
        start_date (_type_, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (_type_, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: daily surface water timeseries data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [abbrev, station_number, usgs_id]):
        raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertsday/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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
    
    print("Retrieving daily surface water timeseries...")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_sw_ts_month(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Request monthly surface water timeseries data

    Args:station_number (str, optional): string, climate data station number. Defaults to None.
        abbrev (_type_, optional): string, tuple or list of surface water station abbreviation. Defaults to None.
        station_number (_type_, optional): string, surface water station number. Defaults to None.
        usgs_id (_type_, optional): string, tuple or list of USGS ID. Defaults to None.
        start_date (_type_, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (_type_, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: monthly surface water timeseries data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [abbrev, station_number, usgs_id]):
        raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertsmonth/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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
    
    print("Retrieving monthly surface water timeseries...")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_sw_ts_wyear(
    abbrev              = None,
    station_number      = None,
    usgs_id             = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Request water year surface water timeseries data

    Args:station_number (str, optional): string, climate data station number. Defaults to None.
        abbrev (_type_, optional): string, tuple or list of surface water station abbreviation. Defaults to None.
        station_number (_type_, optional): string, surface water station number. Defaults to None.
        usgs_id (_type_, optional): string, tuple or list of USGS ID. Defaults to None.
        start_date (_type_, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (_type_, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: annual surface water timeseries data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [abbrev, station_number, usgs_id]):
        raise TypeError("Invalid 'abbrev', 'station_number', or 'usgs_id' parameters")

    #  base API URL
    base =  "https://dwr.state.co.us/Rest/GET/api/v2/surfacewater/surfacewatertswateryear/?"

    # collapse abbreviation list, tuple, vector of site_id into query formatted string
    abbrev = collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    # collapse USGS ID list, tuple, vector of site_id into query formatted string
    usgs_id = collapse_vector(
        vect = usgs_id, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
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

    print("Retrieving water year surface water timeseries...")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
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

    # If all inputs are None, then return error message
    if all(i is None for i in [aoi, abbrev, county, division, gnis_id, usgs_id, water_district, wdid]):
        raise TypeError("Invalid 'aoi', 'abbrev', 'county', 'division', 'gnis_id', 'usgs_id', 'water_district', or 'wdid' parameters")

    # base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrystation/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # collapse site_id list, tuple, vector of site_id into query formatted string
    abbrev = collapse_vector(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_telemetry_ts(
    abbrev              = None,
    parameter           = "DISCHRG",
    start_date          = None,
    end_date            = None,
    timescale           = "day",
    include_third_party = True,
    api_key             = None
    ):
    """Request Telemetry station timeseries data

    Args:
        abbrev (str, optional): Station abbreviation. Defaults to None.
        parameter (str, optional): Indicating which telemetry station parameter should be retrieved. Default is "DISCHRG" (discharge), all parameters are not available at all telemetry stations.. Defaults to "DISCHRG".
        start_date (str, optional): Date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): Date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): Data timescale to return, either "raw", "hour", or "day". Defaults to "day".
        include_third_party (bool, optional): Boolean, indicating whether to retrieve data from other third party sources if necessary. Defaults to True.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of telemetry station timeseries data
    """

    # if no abbreviation is given, return error
    if abbrev is None:
        raise TypeError("Invalid 'abbrev' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrytimeseries" + timescale + "/?"

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )
    
    # Set correct name of date field for querying raw data
    if timescale == "raw": 
        # raw date field name
        date_field = "measDateTime"
    else:
        # hour and day date field name
        date_field = "measDate"
    
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
    
    print("Retrieving telemetry station timeseries data\nTimescale:", timescale)

    # Loop through pages until last page of data is found, binding each responce dataframe together
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")
        
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

    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        wdid (str, optional): WDID code of water right. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights net amounts data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [aoi, county, division, water_district, wdid]):
        raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = check_aoi(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights transactions. Defaults to None.
        water_district (str, optional): Water district to query for water rights transactions. Defaults to None.
        wdid (str, optional): WDID code of water right transaction. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights transactions data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [aoi, county, division, water_district, wdid]):
        raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/transaction/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = check_aoi(
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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def get_call_analysis_wdid(
    wdid                = None,
    admin_no            = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Return call analysis by WDID from analysis services API

    Args:
        wdid (str, optional): DWR WDID unique structure identifier code. Defaults to None.
        admin_no (str, int optional): Water Right Administration Number. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of call services by WDID
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [wdid, admin_no]):
        raise TypeError("Invalid 'wdid' and 'admin_no' parameters.\nPlease enter a 'wdid' and 'admin_no' to retrieve call analysis data")

    # convert int admin_no to str
    if(isinstance(admin_no, (int))):
        admin_no = str(admin_no)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/callanalysisbywdid/?"
    
    # parse start_date into query string format
    start = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
        )

    # parse end_date into query string format
    end = parse_date(
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

    print("Retrieving call anaylsis data by WDID")

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

    Args:
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        gnis_name (str, optional): GNIS Name to query and retrieve DWR source route frameworks. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of source route framework
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [division, gnis_name, water_district]):
        raise TypeError("Invalid 'division', 'gnis_name' or 'water_district' parameters.\nPlease enter a 'division', 'gnis_name' or 'water_district' to retrieve  DWR source route framework data")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/analysisservices/watersourcerouteframework/?"
    
    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

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

        # make API call
        try:
            cdss_req = requests.get(url, timeout = 4)
            cdss_req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("\n" + "HTTP Error:\n" + errh, "\n")
            print("Client response:\n" + errh.response.text, "\n")
        except requests.exceptions.ConnectionError as errc:
            print("\n" + "Connection Error:\n" + errc, "\n")
            print("Client response:\n" + errc.response.text, "\n")
        except requests.exceptions.Timeout as errt:
            print("\n" + "Timeout Error:\n" + errt, "\n")
            print("Client response:\n" + errt.response.text, "\n")
        except requests.exceptions.RequestException as err:
            print("\n" + "Exception raised:\n" + err, "\n")
            print("Client response:\n" + err.response.text,  "\n")

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

def parse_date(
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

def collapse_vector(
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

def extract_coords(
    aoi = None
    ):

    """Internal function for extracting XY coordinates from aoi arguments
    Function takes in a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries of spatial objects,
    and returns a list of length 2, indicating the XY coordinate pair. 
    If the object provided is a Polygon/LineString/LinearRing, the function will return the XY coordinates of the centroid of the spatial object.

    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
    
    Returns:
        list object: list object of an XY coordinate pair
    """
    # if None is passed to 'aoi', return None
    if aoi is None: 

        return None

    # if 'aoi' is NOT none, extract XY coordinates from object
    else:

        # make sure 'aoi' is one of supported types
        if(isinstance(aoi, (list, tuple, geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame, pd.core.frame.DataFrame)) is False):
            raise Exception(("Invalid 'aoi' argument, 'aoi' must be one of the following:\n" + 
            "List/Tuple of an XY coordinate pair\n" +
            "2 column XY Pandas DataFrame\n" +
            "Geopandas GeoDataFrame containing a Polygon, LineString, LinearRing, or Point\n" +
            "Geopandas GeoSeries containing a Point\n"))

        # check if aoi is a list or tuple
        if(isinstance(aoi, (list, tuple))):

            if(len(aoi) >= 2):

                # make coordinate list of XY values
                coord_lst = [aoi[0], aoi[1]]

                # return list of coordinates
                return coord_lst

            else:

                # return list of coordinates
                raise Exception(("Invalid 'aoi' argument, 'aoi' must be one of the following:\n" + 
                "List/Tuple of an XY coordinate pair\n" +
                "2 column XY Pandas DataFrame\n" +
                "Geopandas GeoDataFrame containing a Polygon, LineString, LinearRing, or Point\n" +
                "Geopandas GeoSeries containing a Point\n"))

        # check if aoi is a geopandas geoseries or geodataframe 
        if(isinstance(aoi, (geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame))):

            # convert CRS to 5070
            aoi = aoi.to_crs(5070)

            # if aoi geometry type is polygon/line/linearRing
            if(["Polygon", 'LineString', 'LinearRing'] in aoi.geom_type.values):

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

                # return list of coordinates
                return coord_lst

            # if aoi geometry type is point
            if("Point" in aoi.geom_type.values):
                # checking if point is geopandas Geoseries
                if(isinstance(aoi, (geopandas.geoseries.GeoSeries))):

                    # convert to 4326, and extract lat/lng from Pandas GeoSeries
                    lng = float(aoi.to_crs(4326).apply(lambda p: p.x))
                    lat = float(aoi.to_crs(4326).apply(lambda p: p.y))
                    
                    # lng, lat coordinates
                    coord_lst = [lng, lat]

                    # return list of coordinates
                    return coord_lst

                # checking if point is geopandas GeoDataFrame
                if(isinstance(aoi, (geopandas.geodataframe.GeoDataFrame))):

                    # convert to 4326, and extract lat/lng from Pandas GeoDataFrame
                    lng = float(aoi.to_crs(4326).apply(lambda p: p.x)[0])
                    lat = float(aoi.to_crs(4326).apply(lambda p: p.y)[0])
                            
                    # lng, lat coordinates
                    coord_lst = [lng, lat]

                    # return list of coordinates
                    return coord_lst
                    
        # check if aoi is a Pandas dataframe
        if(isinstance(aoi, (pd.core.frame.DataFrame))):
            
            # extract first and second columns
            lng = float(aoi.iloc[:, 0])
            lat = float(aoi.iloc[:, 1])

            # lng, lat coordinates
            coord_lst = [lng, lat]

            # return list of coordinates
            return coord_lst

def check_radius(
    aoi    = None,
    radius = None
    ):

    """Internal function for radius argument value is within the valid value range for location search queries. 

    Args:
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
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

def check_aoi(
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
        aoi (list, tuple, DataFrame, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
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
        coord_df = extract_coords(aoi = aoi)
        
        # check radius is valid and fix if necessary
        radius = check_radius(
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