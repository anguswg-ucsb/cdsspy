import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

# from cdsspy.utils import utils2
# from cdsspy.cdsspy2 import utils
from cdsspy import utils

def get_water_rights_netamount(
    aoi                 = None,
    radius              = None, 
    county              = None,
    division            = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):
    """Return water rights net amounts data

    Make a request to the /waterrights/netamount endpoint to retrieve water rights net amounts data via a spatial search or by county, division, water district, or WDID, within a given date range (start and end dates).
    Returns current status of a water right based on all of its court decreed actions.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights. Defaults to None.
        water_district (str, optional): Water district to query for water rights. Defaults to None.
        wdid (str, optional): WDID code of water right. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights net amounts data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving water rights net amounts data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

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

    # mask data if necessary
    data_df = utils._aoi_mask(
        aoi = aoi,
        pts = data_df
        )

    return data_df

def get_water_rights_trans(
    aoi                 = None,
    radius              = None, 
    county              = None,
    division            = None,
    water_district      = None,
    wdid                = None,
    api_key             = None
    ):
    """Return water rights transactions data

    Make a request to the /waterrights/transaction endpoint to retrieve water rights transactions data via a spatial search or by county, division, water district, or WDID, within a given date range (start and end dates).
    Returns List of court decreed actions that affect amount and use(s) that can be used by each water right.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None, and if an aoi is given, the radius will default to a 20 mile radius.
        county (str, optional): County to query for water rights. Defaults to None.
        division (int, str, optional):  Water division to query for water rights transactions. Defaults to None.
        water_district (str, optional): Water district to query for water rights transactions. Defaults to None.
        wdid (str, optional): WDID code of water right transaction. Defaults to None.
        api_key (str, optional): API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of water rights transactions data
    """

    # # If all inputs are None, then return error message
    # if all(i is None for i in [aoi, county, division, water_district, wdid]):
    #     raise TypeError("Invalid 'aoi', 'county', 'division', 'water_district', or 'wdid' parameters")

    # list of function inputs
    input_args = locals()

    # check function arguments for missing/invalid inputs
    arg_lst = utils._check_args(
        arg_dict = input_args,
        ignore   = ["api_key"],
        f        = all
        )
    
    # if an error statement is returned (not None), then raise exception with dynamic error message and stop function
    if arg_lst is not None:
        raise Exception(arg_lst)

    #  base API URL
    base = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/transaction/?"

    # check and extract spatial data from 'aoi' and 'radius' args for location search query
    aoi_lst = utils._check_aoi(
        aoi    = aoi,
        radius = radius
        )

    # lat/long coords and radius
    lng    = aoi_lst[0]
    lat    = aoi_lst[1]
    radius = aoi_lst[2]

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving water rights transactions data")

    # Loop through pages until last page of data is found, binding each response dataframe together
    while more_pages == True:

        # create query URL string
        url = (
            f'{base}format=json&dateFormat=spaceSepToSeconds'
            f'&county={county or ""}'
            f'&division={division or ""}'
            f'&waterDistrict={water_district or ""}'
            f'&wdid={wdid or ""}'
            f'&latitude={lat or ""}' 
            f'&longitude={lng or ""}' 
            f'&radius={radius or ""}' 
            f'&units=miles' 
            f'&pageSize={page_size}&pageIndex={page_index}'
            )

        # If an API key is provided, add it to query URL
        if api_key is not None:
            # Construct query URL w/ API key
            url = url + "&apiKey=" + str(api_key)

        # make API call w/ error handling
        cdss_req = utils._parse_gets(
            url      = url, 
            arg_dict = input_args,
            ignore   = None
            )

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

    # mask data if necessary
    data_df = utils._aoi_mask(
        aoi = aoi,
        pts = data_df
        )
    
    return data_df