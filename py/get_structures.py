import pandas as pd
import requests
import numpy as np


def get_structures(
    county              = None,
    division            = None,
    gnis_id             = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/structures/?"

    # if a list of WDIDs, collapse list
    if type(wdid) == list:

        wdid = [str(x) for x in wdid]

        # join list of county names into single string seperated by plus sign
        wdid = "%2C+".join(wdid)
    
        # replace white space w/ plus sign
        wdid  = wdid.replace(" ", "%2C+")
    else:
        # if wdid is an int or float, convert to string
        if type(wdid) == int or type(wdid) == float:
            wdid = str(wdid)
            print("converted int/float to string")
            print(wdid)
        if type(wdid) == str:
            # replace white space w/ plus sign
            wdid  = wdid.replace(" ", "%2C+")

    # maximum records per page
    page_size  = 50000
    # page_size  = 100

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

        # bind data from this page
        data_df = pd.concat([data_df, cdss_df])
        
        # Check if more pages to get to continue/stop while loop
        if(len(cdss_df.index) < page_size): 
            more_pages = False
        else:
            page_index += 1

    return data_df