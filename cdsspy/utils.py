import pandas as pd
import requests
import datetime
import geopandas
import shapely
import pyproj

def _check_args(
        arg_dict = None, 
        ignore   = None,
        f        = any
        ):
    """Check all arguments of a function for any/all NULL values
    
    Internal function for checking a function arguments for any/all invalid/missing arguments necessary to the function it is called within
    
    Args:
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
        f (built-in function): Built in function "any" or "all" to indicate whether to check for "any" or "all" None argument. 
            If "any" then if any of the function arguments are None, then an error is thrown.
            If "all" then all relevant arguments must be None for an error to be thrown. Defaults to any.

    Returns:
        string: error statement with any/all None arguments listed, or None if no error is thrown by None values
    """

    # if no function arguments are given, throw an error
    if arg_dict is None:
        raise Exception("provide a list of function arguments by calling 'locals()', within another function")

    # argument dictionary key/values as lists
    key_lst  = list(arg_dict.keys())
    val_lst  = list(arg_dict.values())

    # if certain arguments are specifically supposed to be ignored
    if ignore is not None:
        
        # remove specifically ignorged arguments
        ignored_lst = [i for i, x in enumerate(key_lst) if x not in ignore]

        # keys and values of arguments to keep
        key_args        = [key_lst[i] for i in ignored_lst]
        val_args        = [val_lst[i] for i in ignored_lst]
    else:
        
        # if no arguments are ignored, keep all argument key/values
        key_args        = key_lst
        val_args        = val_lst

    # if any/all arguments are None, return an error statement. Otherwise return None if None check is passed
    if(f(i is None for i in val_args)):
        # check where in remaining arguments the value is None, and get the index of missing arguments
        idx_miss = [i for i in range(len(val_args)) if val_args[i] == None]

        # return the argument names of None arguments
        key_miss = ", ".join(["'"+key_lst[i]+"'" for i in idx_miss])

        # error print statement
        err_msg = "Invalid or missing " + key_miss + " arguments"

        return err_msg
    else:
        return None
    
def _align_wcid(
        x       = None, 
        default = None
        ):
    """Set wc_identifier name to releases or diversions

    Args:
        x (str): Water class identifier. Defaults to None 
        default (str, int, bool, Nonetype, optional):  value to return if "x" argument is None. Defaults to None.

    Returns:
        string: wc_identifier equaling either "diversion", "release", or a properly formatted water class identifier string
    """
    # if x is NULL/ not given, return "default"
    if x is None:
        return default
    
    # check if x is not in any of diversion/release lists
    if x not in ["diversion", "diversions", "div", "divs", "d", 
                "release", "releases", "rel", "rels", "r"]:
        
        # format wcidentifer query
        x = "+".join([i.replace(":", "%3A") for i in x.split(" ")])

    # if x in the diversions list
    if x in ["diversion", "diversions", "div", "divs", "d"]:
        x = "diversion"

    # if x in the releases list
    if x in ["release", "releases", "rel", "rels", "r"]:
        x = "release"

    x = "*" + x + "*"

    return x
def _valid_divrectype(
        divrectype = None
        ):

    # check if type is NULL, default timescale to "day"
    if divrectype is None:

        divrectype = None

        return divrectype
    
    # list of available divrectypes
    divrectype_lst <- ["DivComment", "DivTotal", "RelComment", "RelTolal", "StageVolume", "WaterClass"]

    # if a divrectype argument is provided (not NULL)
    if divrectype is not None:

        # lowercase divrectype_lst 
        low_lst = [i.lower() for i in divrectype_lst]

        # if divrectype matches any of the list, return correctly formatted divrectype
        if divrectype.lower() in low_lst:
            
            divrectype = divrectype_lst[low_lst.index(divrectype.lower())]
        

        # check if divrectype is a valid divrectype
        if divrectype.lower() not in low_lst and divrectype not in divrectype_lst:
            raise Exception((
                f"Invalid `divrectype` argument: '{divrectype}'",
                f"\nPlease enter one of the following valid 'divrectype' arguments: \n{divrectype_lst}" 
                ))

    return(divrectype)

def _valid_timesteps(
        timestep = None
        ):
    
    # list of valid timescales
    day_lst       = ["day", "days", "daily", "d"]
    month_lst     = ["month", "months", "monthly", "mon", "mons", "m"]
    year_lst      = ['year', 'years', 'yearly', 'annual', 'annually', 'yr', 'y']

    timestep_lst  = [day_lst, month_lst, year_lst]

    # check if type is None, default timescale to "day"
    if timestep is None:
        # set timescale to "day"
        timestep = "day"

    # convert timescale to lowercase
    timestep = timestep.lower()
    
    # check if type is correctly inputed
    if timestep not in timestep_lst: 
        raise Exception((
            f"Invalid `timestep` argument: '{timestep}'",
            f"\nPlease enter one of the following valid timesteps:\nDay: {day_lst}\nMonth: {month_lst}\nYear: {year_lst}" 
            ))
    
    # check if given timestep is in day_lst and set timestep to "day"
    if timestep in day_lst:

        # set timescale to "day"
        timestep = "day"

    # check if given timestep is in month_lst and set timestep to "month"
    if timestep in month_lst:
        
        # set timescale to "mohth"
        timestep = "month"

    # check if given timestep is in month_lst and set timestep to "month"
    if timestep in year_lst:
        
        # set timescale to "year"
        timestep = "year"

    return timestep

def _parse_date(
    date   = None,
    start  = True,
    format =  "%m-%d-%Y"
    ):

    # if the date is the starting date
    if start == True:

        # if no start_date is given, default to 1900-01-01
        if date is None:
            date = "1900-01-01"
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
            date = date.strftime(format)
            date = date.replace("-", "%2F")
        else:
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
            date = date.strftime(format)
            date = date.replace("-", "%2F") 

    # if date is the ending date
    else:

        # if no end date is given, default to current date
        if date is None: 
            date   = datetime.date.today()
            date   = date.strftime(format)
            date   = date.replace("-", "%2F")
        else:
            date   = datetime.datetime.strptime(date, '%Y-%m-%d')
            date   = date.strftime(format)
            date   = date.replace("-", "%2F")

    return date

def _collapse_vector(
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

def _batch_dates(
        start_date = None,
        end_date   = None
        ):
    
    """Create yearly date ranges to make batch GET requests

    Internal function for extracting necessary yearly start and end dates to make a batch of smaller GET requests, instead 1 large date range. 
    Allows for larger date ranges to be queried from the CDSS API without encountering a server side error.

    Args:
        start_date (str): starting date in YYYY-MM-DD format. Default is None which defaults start_date to "1900-01-01".
        end_date (str): ending date in YYYY-MM-DD format. Default is None which defaults to the current date.
    
    Returns:
        list: returns list of date range lists
    """

    # set default start date if None is given 
    if start_date is None:
        start_date = "1900-01-01"

    # set default end_date if None is given 
    if end_date is None:
        end_date   = datetime.date.today()
        end_date   = end_date.strftime('%Y-%m-%d')

    # starting and ending years
    start_year = int(start_date[:4])
    end_year   = int(end_date[:4])

    # empty list add date intervals to
    lst = []

    # if dates are multiple years apart, break into yearly date intervals
    if start_year != end_year:

        # start_date to end of first year
        lst.append((start_date, f"{start_year}-12-31"))

        # yearly intervals
        for y in range(start_year + 1, end_year):
            lst.append((f"{y}-01-01", f"{y}-12-31"))

        # portion of the last year
        lst.append((f"{end_year}-01-01", end_date))

    # if dates are within the same year, just return start_date to end_date
    else:
        lst.append((start_date, end_date))

    return(lst)

def _get_error_handler(
    url      = None
    ):

    """ Make GET requests and return the responses

    Internal function for making get request and returning unsuccesful response text. 
    Used within a try, except block within _parse_gets() function.

    Args:
        url (str, optional): URL of the request
    
    Returns:
        requests.models.Response: returns results of attempted get request   
    """

    # If NO url is given
    if(url is None):
        raise Exception('Please provide a URL to perform a get request')
    
    # make API call

    # attempt GET request
    req_attempt = requests.get(url)

    # if request is 200 (OK), return JSON content data
    if req_attempt.status_code == 200:
        # return successful response
        return req_attempt
    else:
        # return req_attempt.text
        raise Exception(req_attempt.text)
    

def _parse_gets(
        url      = None, 
        arg_dict = None, 
        ignore   = None
        ):
    
    """ Makes GET requests and dynamically handle errors 

    Internal function for handling GET requests and associated errors.
    Function will try to make a GET request, and if an error occurs, the function 
    will return an error message with relevenant information detailing the error 
    and the inputs that led to the error.

    Args:
        url (str): URL of the request
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
    
    Returns:
        requests response: returns results of attempted get request 
    """

    # try to make GET request and error handling unsuccessful requests
    try:
        # attempt GET request
        req = _get_error_handler(url = url)

        return(req)
    
    except Exception as e:

        # if an error occurred, use _query_error() to format a helpful error message to user
        raise Exception(_query_error(
            arg_dict = arg_dict,
            url      = url,
            ignore   = ignore,
            e_msg    = e
            )
            )
    
def _query_error(
        arg_dict = None,
        url      = None,
        ignore   = None,
        e_msg    = None
        ):
    """GET Request Error message handler

    Internal function for generating dynamic error messages for failed GET requests.
    Designed to be called within another function and print out the functions input arguments.

    Args:
        arg_dict (dict): list of function arguments by calling locals() within a function. Defaults to None.
        url (str): URL of the request. Defaults to None.
        ignore (list, optional):  List of function arguments to ignore None check. Defaults to None.
        e_msg (exception, str): exception message or string message that should be pointed to as the original error message. Defaults to None.

    Returns:
        str: error message that includes the query inputs that led to the error, the requested URL, and the original error message
    """

    # if no function arguments are given, throw an error
    if arg_dict is None:
        raise Exception("provide a list of function arguments by calling 'locals()', within another function")
    
    # argument dictionary key/values as lists
    key_lst  = list(arg_dict.keys())
    val_lst  = list(arg_dict.values())

    # if certain arguments are specifically supposed to be ignored
    if ignore is not None:
        
        # remove specifically ignorged arguments
        ignored_lst = [i for i, x in enumerate(key_lst) if x not in ignore]

        # keys and values of arguments to keep
        key_args        = [key_lst[i] for i in ignored_lst]
        val_args        = [val_lst[i] for i in ignored_lst]
    else:
        
        # if no arguments are ignored, keep all argument key/values
        key_args        = key_lst
        val_args        = val_lst

    # query_dict = dict(zip(key_args, val_args))

    q_lst = []

    for i in range(len(key_args)):
            q_lst.append(f'{key_args[i]}: {val_args[i]}')

    q_msg = ("DATA RETRIEVAL ERROR\nQuery:\n" + '\n'.join(q_lst) +
            "\nRequested URL: " + url + 
            "\n\n" + "Original error message: " + "\n-----------------------\n\n" + str(e_msg)
            )

    return q_msg

def _get_error_handler2(
    url     = None
    ):

    """
    Internal function for making get request and error handling unsuccessful requests

    Args:
        url (str, optional): URL of the request
    
    Returns:
        requests.models.Response: returns results of attempted get request   
    """

    # If NO url is given
    if(url is None):
        raise Exception('Please provide a URL to perform a get request')
    
    # make API call
    try:
        # attempt GET request
        req_attempt = requests.get(url)

        req_attempt.raise_for_status()

        # return successful response
        return req_attempt

    except requests.exceptions.HTTPError as errh:
        print("HTTP Error:\n", errh)
        print("\nClient response:\n", errh.response.text)
        raise
    except requests.exceptions.ConnectionError as errc:
        print("Connection Error:\n", errc)
        print("\nClient response:\n", errc.response.text)
        raise
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:\n", errt)
        print("\nClient response:\n", errt.response.text)
        raise
    except requests.exceptions.RequestException as err:
        print("Exception raised:\n", err)
        print("\nClient response:\n", err.response.text)
        raise

def _aoi_error_msg():
    """
    Function to return error message to user when aoi is not valid.
    Returns:
        string: print statement for aoi errors
    """

    msg = ("\nInvalid 'aoi' argument, 'aoi' must be one of the following:\n" + 
    "1. List/Tuple of an XY coordinate pair\n" +
    "2. Dictionary with X and Y keys\n" +
    "3. Pandas DataFrame containing XY coordinates\n" +
    "4. a shapely Point/Polygon/LineString\n" +
    "5. Geopandas GeoDataFrame containing a Polygon/LineString/LinearRing/Point geometry\n" +
    "6. Geopandas GeoSeries containing a Polygon/LineString/Point geometry\n")

    return msg

def _check_coord_crs(epsg_code, lng, lat):

    """Function that checks if a set of longitude and latitude points are within a given EPSG space.

    Returns:
        boolean: True if the coordinates are within the provided EPSG space, False otherwise.
    """
    # given crs epsg code
    crs = pyproj.CRS.from_user_input(epsg_code)

    # if lng/lat fall within CRS space
    if((crs.area_of_use.south <= lat <= crs.area_of_use.north) and (crs.area_of_use.west <= lng <= crs.area_of_use.east)):
        crs_check = True
    else:
        crs_check = False

    return crs_check

def _extract_shapely_coords(aoi):
    """Function for extracting coordinates from a shapely Polygon/LineString/Point
    Internal helper function used in location search queries.

    Args:
        aoi (shapely Polygon/LineString/Point): shapely Polygon/LineString/Point object to extract coordinates from

    Returns:
        list: list of string coordinates with precision of 5 decimal places
    """

    # ensure that the shapely geometry is either a Polygon, LineString or a Point
    if(isinstance(aoi, (shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString, shapely.geometry.point.Point))):

        # if geometry is Polygon or Linestring
        if(isinstance(aoi, (shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString))):
            # extract lng/lat coords
            lng = aoi.centroid.x
            lat = aoi.centroid.y

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # Valid coords in correct CRS space
            if(_check_coord_crs(epsg_code = 4326, lng = lng, lat = lat)):

                # round coordinates to 5 decimal places
                coord_lst = [f'{num:.5f}' for num in coord_lst]

                # return list of coordinates
                return coord_lst

            else:
                raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")

        # if geometry is a Point
        if(isinstance(aoi, (shapely.geometry.point.Point))):
            # extract lng/lat coords
            lng = aoi.x
            lat = aoi.y
            
            # lng, lat coordinates
            coord_lst = [lng, lat]

            # Valid coords in correct CRS space
            if(_check_coord_crs(epsg_code = 4326, lng = lng, lat = lat)):

                # round coordinates to 5 decimal places
                coord_lst = [f'{num:.5f}' for num in coord_lst]

                # return list of coordinates
                return coord_lst

            else:
                raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")
    else:
        raise Exception("Invalid 'aoi' shapely geometry, must be either a shapely Polygon, LineString or Point")



def _extract_coords(
    aoi = None
    ):

    """Internal function for extracting XY coordinates from aoi arguments
    Function takes in a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries of spatial objects,
    and returns a list of length 2, indicating the XY coordinate pair. 
    If the object provided is a Polygon/LineString/LinearRing, the function will return the XY coordinates of the centroid of the spatial object.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
    
    Returns:
        list object: list object of an XY coordinate pair
    """
    # if None is passed to 'aoi', return None
    if aoi is None: 

        return None

    # if 'aoi' is NOT none, extract XY coordinates from object
    else:

        # make sure 'aoi' is one of supported types
        if(isinstance(aoi, (list, tuple, dict, geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame, 
        pd.core.frame.DataFrame, shapely.geometry.polygon.Polygon, shapely.geometry.linestring.LineString, shapely.geometry.point.Point)) is False):
            raise Exception(_aoi_error_msg())

        # check if aoi is a list or tuple
        if(isinstance(aoi, (list, tuple))):

            if(len(aoi) >= 2):

                # make coordinate list of XY values
                coord_lst = [aoi[0], aoi[1]]

                # round coordinates to 5 decimal places
                coord_lst = [float(num) for num in coord_lst]

                # Valid coords in correct CRS space
                if(_check_coord_crs(epsg_code = 4326, lng = coord_lst[0], lat = coord_lst[1])):

                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

                else:
                    raise Exception("Invalid 'aoi' CRS, must convert 'aoi' CRS to epsg:4326")

            else:

                # return list of coordinates
                raise Exception(_aoi_error_msg())

        # check if aoi is a shapely Polygon/LineString/Point
        if("shapely" in str(type(aoi))):

            # extract coordinates from shapely geometry objects
            coord_lst = _extract_shapely_coords(aoi = aoi)

            return coord_lst 

        # check if aoi is a geopandas geoseries or geodataframe 
        if(isinstance(aoi, (geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame))):
            
            if(len(aoi) > 1):
                raise Exception(_aoi_error_msg())

            # convert CRS to 5070
            aoi = aoi.to_crs(5070)

            # if aoi geometry type is polygon/line/linearRing
            if(["Polygon", 'LineString', 'LinearRing'] in aoi.geom_type.values):

                # checking if point is geopandas Geoseries
                if(isinstance(aoi, (geopandas.geoseries.GeoSeries))):

                    # get centroid of polygon, and convert to 4326 and add lng/lat as column
                    lng = float(aoi.centroid.to_crs(4326).geometry.x)
                    lat = float(aoi.centroid.to_crs(4326).geometry.y)

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

                # checking if point is geopandas GeoDataFrame
                if(isinstance(aoi, (geopandas.geodataframe.GeoDataFrame))):

                    # get centroid of polygon, and convert to 4326 and add lng/lat as column
                    aoi["lng"] = aoi.centroid.to_crs(4326).map(lambda p: p.x)
                    aoi["lat"] = aoi.centroid.to_crs(4326).map(lambda p: p.y)

                    # subset just lng/lat cols
                    aoi_coords = aoi.loc[ : , ['lng', 'lat']]

                    # extract lat/lng from centroid of polygon
                    lng = float(aoi_coords["lng"])
                    lat = float(aoi_coords["lat"])

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst

            # if aoi geometry type is point
            if("Point" in aoi.geom_type.values):

                # checking if point is geopandas Geoseries
                if(isinstance(aoi, (geopandas.geoseries.GeoSeries))):

                    # convert to 4326, and extract lat/lng from Pandas GeoSeries
                    lng = float(aoi.to_crs(4326).x)
                    lat = float(aoi.to_crs(4326).y)

                    # lng, lat coordinates
                    coord_lst = [lng, lat]
                    
                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]
                    
                    # return list of coordinates
                    return coord_lst

                # checking if point is geopandas GeoDataFrame
                if(isinstance(aoi, (geopandas.geodataframe.GeoDataFrame))):

                    # convert to 4326, and extract lat/lng from Pandas GeoDataFrame
                    lng = float(aoi.to_crs(4326)['geometry'].x)
                    lat = float(aoi.to_crs(4326)['geometry'].y)
                
                    # lng, lat coordinates
                    coord_lst = [lng, lat]

                    # round coordinates to 5 decimal places
                    coord_lst = [f'{num:.5f}' for num in coord_lst]

                    # return list of coordinates
                    return coord_lst
                    
        # check if aoi is a Pandas dataframe
        if(isinstance(aoi, (pd.core.frame.DataFrame))):
            
            # extract first and second columns
            lng = float(aoi.iloc[:, 0])
            lat = float(aoi.iloc[:, 1])

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # round coordinates to 5 decimal places
            coord_lst = [f'{num:.5f}' for num in coord_lst]

            # return list of coordinates
            return coord_lst

        # check if aoi is a dictionary
        if(isinstance(aoi, (dict))):
            
            # extract "X" and "Y" dict keys
            lng = float(aoi["X"])
            lat = float(aoi["Y"])

            # lng, lat coordinates
            coord_lst = [lng, lat]
            
            # round coordinates to 5 decimal places
            coord_lst = [f'{num:.5f}' for num in coord_lst]

            # return list of coordinates
            return coord_lst

def _check_radius(
    aoi    = None,
    radius = None
    ):

    """Internal function for radius argument value is within the valid value range for location search queries. 

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None.

    Returns:
        int: radius value between 1-150 miles
    """
    # convert str radius value to int
    if(isinstance(radius, (str))):
        radius = int(radius)

    # if spatial data is provided, check type and try to extract XY coordinates
    if aoi is not None:
        # print("AOI arg is NOT None")

        # if radius is not NULL, and is larger than 150, set to max of 150. if NULL radius is provided with spatial data, default to 150 miles
        if radius is not None:
            # print("radius arg is NOT None")
            
            # if radius value is over max, set to 150
            if(radius > 150):

                # print("radius arg > 150, so set to 150")
                radius = 150

            # if radius value is under min, set to 1
            if(radius <= 0):
                # print("radius arg <= 0, so set to 1")
                radius = 1

        # if no radius given, set to 20 miles
        else:
            # print("radius arg is None, default to 20 miles")
            radius = 20
    else:
        # print("AOI arg is None")
        radius = None
    
    # Return radius value
    return radius


def _check_aoi(
    aoi    = None, 
    radius = None
    ):

    """Internal function for checking AOI and radius arguments are valid for use in location search queries.
    Function takes in a list/tuple of an XY coordinate pair, a Pandas Dataframe, or a Geopandas GeoDataFrame/GeoSeries of spatial objects,
    along with a radius value between 1-150 miles.
    The extracts the necessary coordinates from the given aoi parameter and also makes sure the radius value is within the valid value range. 
    The function then returns a list of length 2, indicating the XY coordinate pair. 
    If the object provided is a Polygon/LineString/LinearRing, the function will return the XY coordinates of the centroid of the spatial object.

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        radius (int, str, optional): radius value between 1-150 miles. Defaults to None.

    Returns:
        list: list containing the latitude, longitude, and radius values to use for location search queries.
    """

    # convert str radius value to int
    if(isinstance(radius, (str))):
        radius = int(radius)

    # extract lat/long coords for query
    if(aoi is not None):
        # extract coordinates from matrix/dataframe/sf object
        coord_df = _extract_coords(aoi = aoi)
        
        # check radius is valid and fix if necessary
        radius = _check_radius(
            aoi    = aoi,
            radius = radius
            )

        # lat/long coords
        lng = coord_df[0]
        lat = coord_df[1]
    
    else:
        # if None aoi given, set coords and radius to None
        lng    = None
        lat    = None
        radius = None
    
    # create list to return container longitude, latitude, and radius
    aoi_lst = [lng, lat, radius]
    
    # return lng, lat, radius list
    return aoi_lst  

def _aoi_mask(
    aoi = None,
    pts = None
    ):

    """For location search queries using a polygon, the response data from the CDSS API will be masked to the polygon area, removing any extra points.
    Internal helper function, if aoi is None, then the function will just return the original dataset. 

    Args:
        aoi (list, tuple, dict, DataFrame, shapely geometry, GeoDataFrame, GeoSeries): a list/tuple of an XY coordinate pair, a dictionary with XY keys, a Pandas Dataframe, a shapely Point/Polygon/LineString, or a Geopandas GeoDataFrame/GeoSeries containing a Point/Polygon/LineString/LinearRing. Defaults to None.
        pts (pandas dataframe): pandas dataframe of points that should be masked to the given aoi. Dataframe must contain "utmY" and "utmX" columns

    Returns:
        pandas dataframe: pandas dataframe with all points within the given aoi polygon area
    """

    # if AOI and pts are None, return None
    if all(i is None for i in [aoi, pts]):
        return None

    # if no 'aoi' is given (None), just return original pts data. Default behavior
    if(aoi is None):
        return pts
    
    # check if aoi is a shapely geometry polygon
    if(isinstance(aoi, (shapely.geometry.polygon.Polygon))):

        # if aoi geometry type is polygon/line/linearRing
        if("Polygon" in aoi.geom_type):

            rel_pts = geopandas.overlay(
                geopandas.GeoDataFrame(pts, geometry = geopandas.points_from_xy(pts['utmX'], pts['utmY'])).set_crs(26913).to_crs(4326), 
                geopandas.GeoDataFrame(index = [0], crs = 'epsg:4326', geometry = [aoi]), 
                how = 'intersection'
                )

            # convert geopandas dataframe to pandas dataframe and drop geometry column
            rel_pts = pd.DataFrame(rel_pts.drop(columns='geometry'))

            return rel_pts
        else:
            return pts
    
        
    # check if aoi is a geopandas geoseries or geodataframe 
    if(isinstance(aoi, (geopandas.geoseries.GeoSeries, geopandas.geodataframe.GeoDataFrame))):

        # if aoi geometry type is polygon/line/linearRing
        if(["Polygon"] in aoi.geom_type.values):

            # convert CRS to 4326
            aoi = aoi.to_crs(4326)
            
            # get intersection of points and polygons 
            rel_pts = geopandas.overlay(
                geopandas.GeoDataFrame(pts, geometry = geopandas.points_from_xy(pts['utmX'], pts['utmY'])).set_crs(26913).to_crs(4326), 
                geopandas.GeoDataFrame(aoi.geometry),
                how = 'intersection'
                )

            # convert geopandas dataframe to pandas dataframe and drop geometry column
            rel_pts = pd.DataFrame(rel_pts.drop(columns='geometry'))

            return rel_pts
        else:
            return pts
    else:
        return pts