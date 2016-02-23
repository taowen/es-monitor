import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import traceback

from flask import Flask
from flask import request

import es_query

app = Flask(__name__)

if not os.path.exists('log'):
    os.mkdir('log')
handler = RotatingFileHandler('log/explorer.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.ERROR)
app.logger.addHandler(handler)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/translate', methods=['GET', 'POST'])
def translate():
    if request.method == 'GET':
        sql = request.args.get('q')
    else:
        sql = request.get_data(parse_form_data=False)
    try:
        executor = es_query.create_executor(sql.split(';'))
        es_req = executor.request
        es_req['indices'] = executor.sql_select.from_indices
        resp = {
            'error': None,
            'data': es_req
        }
        return json.dumps(resp, indent=2)
    except:
        etype, value, tb = sys.exc_info()
        resp = {
            'traceback': traceback.format_exception(etype, value, tb),
            'error': str(value),
            'data': None
        }
        return json.dumps(resp, indent=2)


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'GET':
        sql = request.args.get('q')
    else:
        sql = request.get_data(parse_form_data=False)
    es_hosts = request.args.get('by')
    try:
        resp = {
            'error': None,
            'data': es_query.execute_sql(es_hosts, sql)
        }
        return json.dumps(resp, indent=2)
    except:
        etype, value, tb = sys.exc_info()
        resp = {
            'traceback': traceback.format_exception(etype, value, tb),
            'error': str(value),
            'data': None
        }
        return json.dumps(resp, indent=2)


if __name__ == '__main__':
    app.run()
