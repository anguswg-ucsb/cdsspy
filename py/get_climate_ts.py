import pandas as pd
import requests
import datetime

def get_climate_ts_day(
    station_number      = None,
    site_id             = None,
    param               = None,
    start_date          = None,
    end_date            = None,
    api_key             = None
    ):
    
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

    # if no start_date is given, default to 1900-01-01
    if start_date is None: 
        start_date = "1900-01-01"
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        start_date = start_date.strftime("%m-%d-%Y")
        start_date = start_date.replace("-", "%2F")
    else:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        start_date = start_date.strftime("%m-%d-%Y")
        start_date = start_date.replace("-", "%2F")

    # if no end date is given, default to current date
    if end_date is None: 
        end_date   = datetime.date.today()
        end_date   = end_date.strftime("%m-%d-%Y")
        end_date   = end_date.replace("-", "%2F")
    else:
        end_date   = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        end_date   = end_date.strftime("%m-%d-%Y")
        end_date   = end_date.replace("-", "%2F")

    # collapse list, tuple, vector of site_id into query formatted string
    if type(site_id) == list or type(site_id) == tuple:

        site_id = [str(x) for x in site_id]

        # join list of county names into single string seperated by plus sign
        site_id = "%2C+".join(site_id)

        # replace white space w/ plus sign
        site_id = site_id.replace(" ", "%2C+")
    else:
        # if site_id is an int or float, convert to string
        if type(site_id) == int or type(site_id) == float:
            site_id = str(site_id)

        if type(site_id) == str:
            # replace white space w/ plus sign
            site_id = site_id.replace(" ", "%2C+")

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
