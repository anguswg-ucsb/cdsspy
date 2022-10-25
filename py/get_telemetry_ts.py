import pandas as pd
import requests
import datetime

def get_telemetry_ts(
    abbrev              = None,
    parameter           = "DISCHRG",
    start_date          = None,
    end_date            = None,
    timescale           = "day",
    include_third_party = True,
    api_key             = None
    ):

    # if no abbreviation is given, return error
    if abbrev is None:
        return print("Invalid 'abbrev' parameter")

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/telemetrystations/telemetrytimeseries" + timescale + "/?"

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

    # print(start_date)

    # if no end date is given, default to current date
    if end_date is None: 
        end_date   = datetime.date.today()
        end_date   = end_date.strftime("%m-%d-%Y")
        end_date   = end_date.replace("-", "%2F")
    else:
        end_date   = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        end_date   = end_date.strftime("%m-%d-%Y")
        end_date   = end_date.replace("-", "%2F")
        
    # print("Start date: " + start_date)
    # print("Start date: " + end_date)

    # Set correct name of date field for querying raw data
    if timescale == "raw": 
        # raw date field name
        date_field = "measDateTime"
    else:
        # hour and day date field name
        date_field = "measDate"
        
    # maximum records per page
    page_size  = 50000
    # page_size  = 1000

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
        "format=json&dateFormat=spaceSepToSeconds&fields=abbrev%2Cparameter%2C", date_field, "%2CmeasValue%2CmeasUnit",
        "&abbrev=", abbrev,
        "&endDate=", end_date,
        "&startDate=", start_date,
        "&includeThirdParty=", str(include_third_party).lower(),
        "&parameter=", parameter,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )
        
        # join tuble into single string
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