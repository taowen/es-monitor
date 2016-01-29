#!/usr/bin/python

"""
Report odin series using elasticsearch query

"""
import sys
import json
import time
import urllib
import urllib2
import datetime

ES_HOSTS = 'http://10.121.89.8/gsapi'


def collect():
    url = ES_HOSTS + '/gs_plutus_debug_*/_count'
    data = {
        "filter": {
            "range": {
                "timestamp": {
                    "from": (long(time.time()) - 5 * 60) * 1000,
                    "to": long(time.time()) * 1000
                }
            }
        }
    }
    try:
        resp = urllib2.urlopen(url, json.dumps(data)).read()
    except:
        return

    datapoints = []
    ts = int(time.time())
    datapoint = {}
    datapoint["name"] = 'gs_plutus_debug.count'
    datapoint["timestamp"] = ts
    datapoint["value"] = json.loads(resp)['count']
    datapoints.append(datapoint)
    print json.dumps(datapoints)
    sys.stdout.flush()



if __name__ == "__main__":
    sys.exit(collect())
