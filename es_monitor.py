#!/usr/bin/python

"""
Report odin series using elasticsearch query

"""
import base64
import json
import os
import sys
import time
import urllib2
import logging
import logging.handlers

LOGGER = logging.getLogger(__name__)

from es_sql import es_query


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
    try:
        result_map = es_query.execute_sql(es_hosts, sql)
    except urllib2.HTTPError as e:
        LOGGER.exception(e.read())
        sys.exit(1)
    except:
        LOGGER.exception('read datapoint failed')
        sys.exit(1)
    for metric_name, rows in result_map.iteritems():
        for row in rows or []:
            datapoint = {}
            datapoint['timestamp'] = ts
            datapoint['name'] = row.pop('_metric_name', None) or metric_name
            datapoint['value'] = row.pop('value', 0)
            if row:
                tags = {}
                for k, v in row.iteritems():
                    k = to_str(k)
                    if not k.startswith('_'):
                        tags[k] = to_str(v)
                datapoint['tags'] = tags
            datapoints.append(datapoint)
    LOGGER.info('read datapoints: %s' % len(datapoints))
    return datapoints


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    home = os.getenv('HOME')
    if not home:
        home = '/tmp'
    handler = logging.handlers.RotatingFileHandler(os.path.join(home, '.es-monitor.log'), maxBytes=1024 * 1024, backupCount=0)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(handler)
    try:
        url = sys.argv[1] if len(sys.argv) > 1 else 'file:///home/rd/es-monitor/current.conf'
        if url.startswith('file:///'):
            with open(url.replace('file:///', '/'), 'r') as f:
                content = f.read()
        else:
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
    except:
        LOGGER.exception('failed to run')
        sys.exit(1)