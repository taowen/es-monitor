import json
import sys
import logging

from . import es_query


def main():
    logging.basicConfig(level=logging.DEBUG)
    sql = sys.stdin.read()
    result_map = es_query.execute_sql(sys.argv[1], sql)
    print('=====')
    for result_name, rows in result_map.iteritems():
        for row in rows:
            print json.dumps(row)


if __name__ == "__main__":
    main()
