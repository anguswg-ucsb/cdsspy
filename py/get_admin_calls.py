import pandas as pd
import requests
import datetime

def parse_date(
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

def get_admin_calls(
    division            = None,
    location_wdid       = None,
    call_number         = None,
    start_date          = None,
    end_date            = None,
    active              = True,
    api_key             = None
    ):
    """Return active/historic administrative calls data
    Make a request to the api/v2/administrativecalls endpoint to locate active or historical administrative calls by division, location WDID, or call number within a specified date range.

    Args:
        division (int, str, optional): Water division to query for administrative calls. Defaults to None.
        location_wdid (str, optional): call location structure WDID to query for administrative calls. Defaults to None.
        call_number (int, str, optional): unique call identifier to query. Defaults to None.
        start_date (str, optional): string date to request data start point YYYY-MM-DD. Defaults to None, which will return data starting at "1900-01-01".
        end_date (str, optional): string date to request data end point YYYY-MM-DD.. Defaults to None, which will return data ending at the current date.
        active (bool, optional): whether to get active or historical administrative calls. Defaults to True which returns active administrative calls.
        api_key (str, optional): string, API authorization token, optional. If more than maximum number of requests per day is desired, an API key can be obtained from CDSS. Defaults to None.

    Returns:
        pandas dataframe object: dataframe of active/historical administrative calls data
    """

    # if no division, location_wdid, or call number are given, return error
    if division is None and location_wdid is None and call_number is None:
        return print("Invalid 'division', 'location_wdid', or 'call_number' parameters")
    
    # collapse location_wdid list, tuple, vector of site_id into query formatted string
    location_wdid = collapse_vector(
        vect = location_wdid, 
        sep  = "%2C+"
        )

    # parse start_date into query string format
    start_date = parse_date(
        date   = start_date,
        start  = True,
        format = "%m-%d-%Y"
    )

    # parse end_date into query string format
    end_date = parse_date(
        date   = end_date,
        start  = False,
        format = "%m-%d-%Y"
        )

    #  base API URL and print statements
    if active == True:
        print("Retrieving active administrative calls data")
        base = "https://dwr.state.co.us/Rest/GET/api/v2/administrativecalls/active/?"
    else:
        print("Retrieving historical administrative calls data")
        base = "https://dwr.state.co.us/Rest/GET/api/v2/administrativecalls/historical/?"

    # maximum records per page
    page_size = 50000

    # initialize empty dataframe to store data from multiple pages
    data_df = pd.DataFrame()

    # initialize first page index
    page_index = 1

    # Loop through pages until there are no more pages to get
    more_pages = True

    print("Retrieving surface water station data")

    # Loop through pages until last page of data is found, binding each responce dataframe together
    while more_pages == True:
        # create query URL string tuple
        url = (base,
        "format=json&dateFormat=spaceSepToSeconds",
        "&min-dateTimeSet=", start_date,
        "&max-dateTimeSet=", end_date,
        "&division=", division,
        "&callNumber=", call_number,
        "&pageSize=", str(page_size),
        "&pageIndex=", str(page_index)
        )

        # concatenate non-None values into query URL
        url = [x for x in url if x is not None]
        url = "".join(url)

        # Construct query URL w/ location WDID
        if location_wdid is not None:
            url = url + "&locationWdid=" + str(location_wdid)

        # If an API key is provided, add it to query URL
        if api_key is not None:
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