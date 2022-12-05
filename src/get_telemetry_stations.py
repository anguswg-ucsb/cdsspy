import pandas as pd
import requests

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

def get_climate_stations(
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
    
