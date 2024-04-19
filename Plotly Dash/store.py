#############
### store ###
#############

import numpy as np
import os
import pandas as pd
import pickle
import redis


def store_data(leaks, fleet, spot, vppa, flag, vol):    
    leaks_json = pickle.dumps(leaks)
    fleet_json = pickle.dumps(fleet)
    spot_json = pickle.dumps(spot)
    vppa_json = pickle.dumps(vppa)
    flag_json = pickle.dumps(flag)
    vol_json = pickle.dumps(vol)
    # establish redis
    redis_client = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
    # to redis
    redis_client.set('leaks', leaks_json)
    redis_client.set('fleet', fleet_json)
    redis_client.set('spot', spot_json)
    redis_client.set('vppa', vppa_json)
    redis_client.set('flag', flag_json)
    redis_client.set('vol', vol_json)
    # to csv
    #leaks.to_csv('./input_data/leaks.csv')
    #spot.to_csv('./input_data/spot.csv')
    #vppa.to_csv('./input_data/vppa.csv')
    #flag.to_csv('./input_data/flag.csv')
    return