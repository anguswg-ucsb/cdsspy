import datetime
import json
import random

def date_stream():
    # generate current date and a random number
    x = {
        "pid": str(datetime.datetime.now()),
        "rnum": random.randint(0,9)
        }

    # convert dictionary to JSON 
    x = json.dumps(x) 

    return x