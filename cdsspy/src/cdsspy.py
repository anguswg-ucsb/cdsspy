# __init__.py
__version__ = "0.0.1"

import pandas as pd
import requests
import datetime

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

    # if no division, location_wdid, or call number are given, return error
    if division is None and location_wdid is None and call_number is None:
        return print("Invalid 'division', 'location_wdid', or 'call_number' parameters")
    
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
        # create query URL string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&min-dateTimeSet=", start_date,
        "&max-dateTimeSet=", end_date,
        "&division=", division,
        "&callNumber=", call_number,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # Construct query URL w/ location WDID
        if location_wdid is not None:
            url = url + "&locationWdid=" + str(location_wdid)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
    county              = None,
    division            = None,
    station_name        = None,
    site_id             = None,
    water_district      = None,
    api_key             = None
    ):
    """Request Climate Station information

    Args:
        county (str, optional): County to query for climate stations. Defaults to None.
        division (int, str, optional):  Water division to query for climate stations. Defaults to None.
        station_name (str, optional): string, climate station name. Defaults to None.
        site_id (str, tuple, list, optional): string, tuple or list of site IDs. Defaults to None.
        water_district (int, str, optional): Water district to query for climate stations. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.


    Returns:
        pandas dataframe object: dataframe of climate station data
    """
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

        # create query URL string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&county=", county,
        "&division=", division,
        "&stationName=", station_name,
        "&siteId=", site_id,
        "&waterDistrict=", water_district,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # if no site_id and no station_number are given, return error
    if site_id is None and station_number is None:
        return print("Invalid 'site_id' or 'station_number' parameters")

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
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&min-measDate=", start_date,
        "&max-measDate=", end_date,
        "&stationNum=", station_number,
        "&siteId=", site_id,
        "&measType=", param,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        #  concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid `param` argument \nPlease enter one of the following valid parameters: \nEvap, FrostDate, MaxTemp, MeanTemp, MinTemp, Precip, Snow, SnowDepth, SnowSWE, Solar, VP, Wind")

    # if no site_id and no station_number are given, return error
    if site_id is None and station_number is None:
        return print("Invalid 'site_id' or 'station_number' parameters")

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
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&min-calYear=", start_year,
        "&max-calYear=", end_year,
        "&stationNum=", station_number,
        "&siteId=", site_id,
        "&measType=", param,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        #  concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create query URL string tuple
        url = (
            base,
            "format=json&dateFormat=spaceSepToSeconds",
            "&county=", county,
            "&wellId=", wellid,
            "&division=", division,
            "&waterDistrict=", water_district,
            "&designatedBasin=", designated_basin,
            "&managementDistrict=", management_district,
            "&pageSize=", str(page_size),
            "&pageIndex=", str(page_index)
            )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # if no well ID is given, return error
    if wellid is None:
        return print("Invalid 'wellid' parameter")

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

        # create query URL string tuple
        url = (
            base,
            "format=json&dateFormat=spaceSepToSeconds",
            "&min-measurementDate=", start,
            "&max-measurementDate=", end,
            "&wellId=", wellid,
            "&pageSize=", str(page_size),
            "&pageIndex=", str(page_index)
            )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create query URL string tuple
        url = (
            base,
            "format=json&dateFormat=spaceSepToSeconds",
            "&county=", county,
            "&wellId=", wellid,
            "&division=", division,
            "&waterDistrict=", water_district,
            "&designatedBasin=", designated_basin,
            "&managementDistrict=", management_district,
            "&pageSize=", str(page_size),
            "&pageIndex=", str(page_index)
            )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create query URL string tuple
        url = (
            base,
            "format=json&dateFormat=spaceSepToSeconds",
            "&wellId=", wellid,
            "&pageSize=", str(page_size),
            "&pageIndex=", str(page_index)
            )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&county=", county,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&division=", division,
        "&waterDistrict=", water_district,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&division=", division,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&managementDistrictName=", management_district,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json",
        "&designatedBasinName=", designated_basin,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json",
        "&parameter=", param,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json",
        "&measType=", param,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json",
        "&divRecType=", divrectype,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

        # create string tuple
        url = (base,
        "format=json",
        "&flag=", flag,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid `table_name` argument \nPlease enter one of the following valid table names: \ncounty\nwaterdistricts\nwaterdivisions\ndesignatedbasins\nmanagementdistricts\ntelemetryparams\nclimateparams\ndivrectypes\nflags")

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
    # if no abbreviation is given, return error
    if wdid is None:
        return print("Invalid 'wdid' parameter")

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

        # create string tuple
        url = (base, 
        "format=json&dateFormat=spaceSepToSeconds",
        "&wcIdentifier=*", wc_identifier,
        "*&min-dataMeasDate=", start_date,
        "&max-dataMeasDate=", end_date,
        "&wdid=", wdid,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
    # if no abbreviation is given, return error
    if wdid is None:
        return print("Invalid 'wdid' parameter")

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
        # create query URL string tuple
        url = (base, 
        "format=json&dateFormat=spaceSepToSeconds",
        "&wcIdentifier=*", wc_identifier,
        "*&min-dataMeasDate=", start_date,
        "&max-dataMeasDate=", end_date,
        "&wdid=", wdid,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # if no abbreviation is given, return error
    if wdid is None:
        return print("Invalid 'wdid' parameter")

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
        # create query URL string tuple
        url = (base, 
        "format=json&dateFormat=spaceSepToSeconds",
        "&wcIdentifier=*", wc_identifier,
        "*&min-dataMeasDate=", start_date,
        "&max-dataMeasDate=", end_date,
        "&wdid=", wdid,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid 'wdid' parameter")

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

        # create string tuple
        url = (base, 
        "format=json&dateFormat=spaceSepToSeconds",
        "&min-dataMeasDate=", start_date,
        "&max-dataMeasDate=", end_date,
        "&wdid=", wdid,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
    county         = None,
    division       = None,
    gnis_id        = None,
    water_district = None,
    wdid           = None,
    api_key        = None
):
    """Return list of administrative structures
    Make a request to the api/v2/structures endpoint to locate administrative structures by division, county, water_district, GNIS, or WDID.
    Args:
        county (str, optional): Indicating the county to query. Defaults to None.
        division (int, str, optional): Indicating the water division to query. Defaults to None.
        gnis_id (str, optional): Water source - Geographic Name Information System ID (GNIS ID). Defaults to None.
        water_district (int, str, optional): Indicating the water district to query. Defaults to None.
        wdid (string, tuple or list, optional): WDIDs code of structure. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of administrative structures
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/?"

    # convert numeric division to string
    if type(division) == int or type(division) == float:
        division = str(division)

    # convert numeric water_district to string
    if type(water_district) == int or type(water_district) == float:
        water_district = str(water_district)

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

        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&county=", (county),
        "&division=", (division),
        "&gnisId=", (gnis_id),
        "&waterDistrict=", (water_district),
        "&wdid=", (wdid),
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # if no site_id and no station_number are given, return error
    if abbrev is None and station_number is None and usgs_id is None:
        return print("Invalid 'abbrev', 'station_number', or 'usgs_id', parameters")

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
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&abbrev=", abbrev,
        "&min-measDate=", start_date,
        "&max-measDate=", end_date,
        "&stationNum=", station_number,
        "&usgsSiteId=", usgs_id,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # if no site_id and no station_number are given, return error
    if abbrev is None and station_number is None and usgs_id is None:
        return print("Invalid 'abbrev', 'station_number', or 'usgs_id', parameters")

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
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&abbrev=", abbrev,
        "&min-calYear=", start_date,
        "&max-calYear=", end_date,
        "&stationNum=", station_number,
        "&usgsSiteId=", usgs_id,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # if no site_id and no station_number are given, return error
    if abbrev is None and station_number is None and usgs_id is None:
        return print("Invalid 'abbrev', 'station_number', or 'usgs_id', parameters")

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
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&abbrev=", abbrev,
        "&min-waterYear=", start_date,
        "&max-waterYear=", end_date,
        "&stationNum=", station_number,
        "&usgsSiteId=", usgs_id,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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

    # collapse site_id list, tuple, vector of site_id into query formatted string
    abbrev = collapse_vector(
        vect = abbrev, 
        sep  = "%2C+"
        )

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrystation/?"

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

        # create query URL string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&abbrev=", abbrev,
        "&county=", county,
        "&division=", division,
        "&gnisId=", gnis_id,
        "&includeThirdParty=true",
        "&usgsStationId=", usgs_id,
        "&waterDistrict=", water_district,
        "&wdid=", wdid,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid 'abbrev' parameter")

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
        
        # create string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&abbrev=", abbrev,
        "&endDate=", end_date,
        "&startDate=", start_date,
        "&includeThirdParty=", str(include_third_party).lower(),
        "&parameter=", parameter,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)
        
        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)
        
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
    county              = None,
    division            = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):
    """Return water rights net amounts data

    Args:
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        wdid (str, optional): WDID code of water right. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights net amounts data
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [county, division, water_district, wdid]):
        return print("Invalid 'county', 'division', 'water_district', or 'wdid' parameters")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount/?"

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
        url = f'{base}format=json&dateFormat=spaceSepToSeconds&county={county or ""}&division={division or ""}&waterDistrict={water_district or ""}&wdid={wdid or ""}&pageSize={page_size}&pageIndex={page_index}'

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
    
wdid                = 45
admin_no            = 4677
start_date          = "1934-03-27"
end_date            = None
api_key             = None

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
# # create query URL string
# url = f'{base}format=json&dateFormat=spaceSepToSeconds&adminNo={admin_no or ""}&endDate={end or ""}&startDate={start or ""}&wdid={wdid or ""}&pageSize={page_size}&pageIndex={page_index}'

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
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of call services by WDID
    """

    # If all inputs are None, then return error message
    if all(i is None for i in [wdid, admin_no]):
        return print("Invalid 'wdid' and 'admin_no' parameters.\nPlease enter a 'wdid' and 'admin_no' to retrieve call analysis data")

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
        url = f'{base}format=json&dateFormat=spaceSepToSeconds&adminNo={admin_no or ""}&endDate={end or ""}&startDate={start or ""}&wdid={wdid or ""}&pageSize={page_size}&pageIndex={page_index}'

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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
        return print("Invalid 'division', 'gnis_name' or 'water_district' parameters.\nPlease enter a 'division', 'gnis_name' or 'water_district' to retrieve  DWR source route framework data")

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
        url = f'{base}format=json&dateFormat=spaceSepToSeconds&division={division or ""}&gnisName={gnis_name or ""}&waterDistrict={water_district or ""}&pageSize={page_size}&pageIndex={page_index}'

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call
        cdss_req = requests.get(url)

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