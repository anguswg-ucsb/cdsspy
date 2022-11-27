import pandas as pd
import requests

def collapse_vector(vect=None, sep="%2C+"):

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
