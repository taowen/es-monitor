#!/usr/bin/python

"""
Report odin series using elasticsearch query

"""
import es_query
import sys
import json
import time
import base64

if __name__ == "__main__":
    metric_name = sys.argv[1]
    sql = base64.b64decode(sys.argv[2])
    rows = es_query.execute_sql(sql)
    ts = int(time.time())
    datapoints = []
    for datapoint in rows:
        datapoint['name'] = metric_name
        datapoint['timestamp'] = ts
        datapoints.append(datapoint)
    print json.dumps(datapoints)
    sys.exit(0 if rows else 1)
