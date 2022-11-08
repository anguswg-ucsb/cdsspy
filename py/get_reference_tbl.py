import pandas as pd
import requests

def get_ref_county(
    county  = None, 
    api_key = None
    ):
    """Return county reference table

    Args:
        county (str, optional): string, (optional) indicating the county to query, if no county is given, entire county dataframe is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        division (str, optional): string, (optional) indicating the division to query, if no division is given, dataframe of all water districts is returned. Defaults to None.
        water_district (str, optional): string, (optional) indicating the water district to query, if no water district is given, dataframe of all water districts is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
    """Return water divisions reference table

    Args:
        division (str, optional): Division to query, if no division is given, dataframe of all water divisions is returned. Defaults to None.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of Colorado water divisions
    """

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
        management_district (str, optional): Indicating the management district to query, if no management district is given, dataframe of all management districts is returned Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        designated_basin (str, optional): Indicating the  designated basin to query character, if no designated basin is given, all designated basins dataframe is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        param (str, optional): Indicating the parameter to query character, if no parameter is given, all parameter dataframe is returned Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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
        param (str, optional): Indicating the climate station parameter to query, if no parameter is given, all parameter dataframe is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

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

def get_ref_divrectypes(
    divrectype   = None, 
    api_key      = None
    ):
    """Return Diversion Record Types reference table
    
    Args:
        divrectype (str, optional): Diversion record type to query, if no divrectype is given, a dataframe with all diversion record types is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of diversion record types reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/divrectypes/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Diversion record types")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create string tuple
        url = (base,
        "format=json",
        "&divRecType=", divrectype,
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

def get_ref_stationflags(
    flag    = None, 
    api_key = None
    ):
    """Return Station Flag reference table
    
    Args:
        flag (str, optional): short code for the flag to query, if no flag is given, a dataframe with all flags is returned. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe: dataframe of diversion record types reference table
    """
    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/referencetables/stationflags/?"

    # maximum records per page
    page_size  = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df    = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True
    
    print("Retrieving reference table: Station flags")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create string tuple
        url = (base,
        "format=json",
        "&flag=", flag,
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


def get_reference_tbl(
    table_name = None,
    api_key    = None
    ):
    """Return Reference Table reference table
    
    Args:
        table_namDee (str, optional): Name of the reference table to return. One of: "county", "waterdistricts", "waterdivisions", "designatedbasins", "managementdistricts", "telemetryparams", "climateparams", "divrectypes", "flags". Defaults to none.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.
    
    Returns:
        pandas dataframe: dataframe of CDSS reference tables
    """
    # list of valid parameters
    tbl_lst = ["county", "waterdistricts", "waterdivisions", "designatedbasins", "managementdistricts", "telemetryparams", "climateparams", "divrectypes", "flags"]

    # if parameter is not in list of valid parameters
    if table_name not in tbl_lst:
        return print("Invalid `table_name` argument \nPlease enter one of the following valid table names: \ncounty\nwaterdistricts\nwaterdivisions\ndesignatedbasins\nmanagementdistricts\ntelemetryparams\nclimateparams\ndivrectypes\nflags")

    # retrieve county reference table
    if table_name == "county":
        ref_table = get_ref_county(
            api_key = api_key
            )
        return ref_table
    
    # retrieve water districts reference table
    if table_name == "waterdistricts":
        ref_table = get_ref_waterdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve water divisions reference table
    if table_name == "waterdivisions":
        ref_table = get_ref_waterdivisions(
            api_key = api_key
            )
        return ref_table

    # retrieve management districts reference table
    if table_name == "managementdistricts":
        ref_table = get_ref_managementdistricts(
            api_key = api_key
            )
        return ref_table

    # retrieve designated basins reference table
    if table_name == "designatedbasins":
        ref_table = get_ref_designatedbasins(
            api_key = api_key
            )
        return ref_table

    # retrieve telemetry station parameters reference table
    if table_name == "telemetryparams":
        ref_table = get_ref_telemetry_params(
            api_key = api_key
            )
        return ref_table

    # retrieve climate station parameters reference table
    if table_name == "climateparams":
        ref_table = get_ref_climate_params(
            api_key = api_key
            )
        return ref_table

    # retrieve diversion record types reference table
    if table_name == "divrectypes":
        ref_table = get_ref_divrectypes(
            api_key = api_key
            )
        return ref_table

    # retrieve station flags reference table
    if table_name == "flags":
        ref_table = get_ref_stationflags(
            api_key = api_key
            )
        return ref_table
