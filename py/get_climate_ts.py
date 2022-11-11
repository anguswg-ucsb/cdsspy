import pandas as pd
import requests
import datetime

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


def get_climate_ts_day(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    """Request daily climate data

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
    """Request monthly climate data

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