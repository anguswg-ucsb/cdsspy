import pandas as pd
import requests
import numpy as np

def get_ref_county(
    county  = None, 
    api_key = None
    ):
    """Return county reference table

    Args:
        county (_type_, optional): string, (optional) indicating the county to query, if no county is given, entire county dataframe is returned. Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        division (_type_, optional): string, (optional) indicating the division to query, if no division is given, dataframe of all water districts is returned. Defaults to None.
        water_district (_type_, optional): string, (optional) indicating the water district to query, if no water district is given, dataframe of all water districts is returned. Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        management_district (_type_, optional): string,  (optional) indicating the management district to query, if no management district is given, dataframe of all management districts is returned Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        designated_basin (_type_, optional): string,  (optional) indicating the  designated basin to query character, if no designated basin is given, all designated basins dataframe is returned. Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        param (_type_, optional): string, (optional) indicating the parameter to query character, if no parameter is given, all parameter dataframe is returned Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        param (_type_, optional): string, (optional) indicating the parameter to query character, if no parameter is given, all parameter dataframe is returned. Defaults to None.
        api_key (_type_, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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