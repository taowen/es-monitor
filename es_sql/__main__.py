import json
import sys

from . import es_query

es_query.DEBUG = True
sql = sys.stdin.read()
result_map = es_query.execute_sql(sys.argv[1], sql)
print('=====')
for result_name, rows in result_map.iteritems():
    for row in rows:
        print json.dumps(row)
