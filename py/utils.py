import datetime
import json
import random

def collapse_vector(
    vect = None, 
    sep  = "%2C+"
    ):

    # if no vector is provided
    if vect is None:
        return print("Invalid Nonetype 'vect' parameter.\nPlease enter a valid vector")

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
            


def date_stream():
    # generate current date and a random number
    x = {
        "pid": str(datetime.datetime.now()),
        "rnum": random.randint(0,9)
        }

    # convert dictionary to JSON 
    x = json.dumps(x) 

    return x