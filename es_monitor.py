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


if __name__ == "__main__":
    url = sys.argv[1]
    cache_key = 'es-monitor-%s' % base64.b64encode(url)
    if os.path.exists(cache_key):
        with open('/tmp/%s' % cache_key) as f:
            content = f.read()
    else:
        resp = urllib2.urlopen(url)
        content = resp.read()
        with open('/tmp/%s' % cache_key, 'w') as f:
            f.write(content)
    lines = content.splitlines()
    es_hosts = lines[0]
    metric_name = lines[1]
    sql = '\n'.join(lines[2:])
    rows = es_query.execute_sql(es_hosts, sql)
    ts = int(time.time())
    datapoints = []
    for row in rows or []:
        datapoint = {'value': row.pop('value', 0)}
        if row:
            datapoint['tags'] = {to_str(k): to_str(v) for k, v in row.iteritems()}
        datapoint['name'] = metric_name
        datapoint['timestamp'] = ts
        datapoints.append(datapoint)
    print json.dumps(datapoints)
    sys.exit(0 if rows else 1)
