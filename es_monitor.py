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
    cache_key = '/tmp/es-monitor-%s' % base64.b64encode(url)
    if os.path.exists(cache_key):
        with open(cache_key) as f:
            content = f.read()
    else:
        resp = urllib2.urlopen(url)
        content = resp.read()
        with open(cache_key, 'w') as f:
            f.write(content)
    lines = content.splitlines()
    es_hosts = lines[0]
    current_sql = []
    metrics = []
    for line in lines[1:]:
        if not line.strip():
            continue
        if line.strip().startswith('>>>'):
            metric_name = line.replace('>>>', '').strip()
            sql = '\n'.join(current_sql)
            current_sql = []
            metrics.append((metric_name, sql))
        else:
            current_sql.append(line)
    datapoints = []
    for metric_name, sql in metrics:
        rows = es_query.execute_sql(es_hosts, sql)
        ts = int(time.time())
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
    print json.dumps(datapoints)