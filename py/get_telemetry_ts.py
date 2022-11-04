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
        abbrev (_type_, optional): string indicating station abbreviation. Defaults to None.
        parameter (str, optional): string . Default is "DISCHRG" (discharge), all parameters are not available at all telemetry stations.. Defaults to "DISCHRG".
        start_date (_type_, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (_type_, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        timescale (str, optional): string indicating data type to return, either "raw", "hour", or "day". Defaults to "day".
        include_third_party (bool, optional): Boolean, indicating whether to retrieve data from other third party sources if necessary. Defaults to True.
        api_key (_type_, optional): string, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS.. Defaults to None.

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
        cdss_df  = cdss_req.json() 
        cdss_df  = pd.DataFrame(cdss_df)
        cdss_df  = cdss_df["ResultList"].apply(pd.Series) 
        
        # convert measDateTime and measDate columns to 'date' and pd datetime type
        if timescale == "raw":
            # convert measDate column to datetime column
            cdss_df['date'] = pd.to_datetime(cdss_df['measDateTime'])

            # remove old measDate column
            del cdss_df['measDateTime']
        else: 
            # convert measDate column to datetime column
            cdss_df['date'] = pd.to_datetime(cdss_df['measDate'])
            
            # remove old measDate column
            del cdss_df['measDate']

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df