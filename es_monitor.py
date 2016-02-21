#!/usr/bin/python

"""
Report odin series using elasticsearch query

"""
import es_query
import sys
import json
import time
import base64
import urllib2
import os


def to_str(val):
    if isinstance(val, basestring):
        return val
    else:
        return str(val)


def query_datapoints(config):
    lines = config.splitlines()
    es_hosts = lines[0]
    sql = ''.join(lines[1:])
    datapoints = []
    ts = int(time.time())
    result_map = es_query.execute_sql(es_hosts, sql)
    for metric_name, rows in result_map.iteritems():
        for row in rows or []:
            datapoint = {'value': row.pop('value', 0)}
            if row:
                tags = {}
                for k, v in row.iteritems():
                    tags[to_str(k)] = to_str(v)
                datapoint['tags'] = tags
            datapoint['name'] = metric_name
            datapoint['timestamp'] = ts
            datapoints.append(datapoint)
    return datapoints


if __name__ == "__main__":
    url = sys.argv[1]
    cache_key = '/tmp/es-monitor-%s' % base64.b64encode(url)
    if os.path.exists(cache_key):
        with open(cache_key) as f:
            content = f.read()
    else:
        resp = urllib2.urlopen(url)
        content = resp.read()
        with open(cache_key, 'w') as f:
            f.write(content)
    print json.dumps(query_datapoints(content))
