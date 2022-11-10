import pandas as pd
import requests
import datetime

def get_climate_stations(
    county              = None,
    division            = None,
    station_name        = None,
    site_id             = None,
    water_district      = None,
    api_key             = None
    ):
    
    # if a list of site_ids, collapse list
    if type(site_id) == list or type(site_id) == tuple:
        site_id = [str(x) for x in site_id]

        # join list of site IDs into single string seperated by plus sign
        site_id = "%2C+".join(site_id)
        
        # replace white space w/ plus sign
        site_id = site_id.replace(" ", "%2C+")
    else:
        # if site_id is an int or float, convert to string
        if type(site_id) == int or type(site_id) == float:
            site_id = str(site_id)

        if type(site_id) == str:
            # replace white space w/ %2C+ sign
            site_id = site_id.replace(" ", "%2C+")

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
    
