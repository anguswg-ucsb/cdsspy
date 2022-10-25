import pandas as pd
import requests
import numpy as np

def get_ref_county(
    county  = None, 
    api_key = None
    ):

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/county/?"

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
        
        # Check if a specific county name was requested
        if county is None:
             # print(url)
            url = base + "format=json&dateFormat=spaceSepToSeconds" + "&county=&pageSize="  + str(page_size) + "&pageIndex=" + str(page_index)
        else:
            # print(url)
            url = base + "format=json&dateFormat=spaceSepToSeconds" + "&county=" +  county +"&pageSize="  + str(page_size) + "&pageIndex=" + str(page_index)
        
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





