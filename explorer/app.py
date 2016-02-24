import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import traceback

import flask

import es_query

app_dir = os.path.dirname(__file__)
app = flask.Flask(__name__, template_folder=app_dir)

if not os.path.exists('log'):
    os.mkdir('log')
handler = RotatingFileHandler('log/explorer.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.ERROR)
app.logger.addHandler(handler)


@app.route('/')
def explorer():
    return flask.render_template('explorer.html')


@app.route('/res/<path:path>')
def send_res(path):
    return flask.send_from_directory(os.path.join(app_dir, 'res'), path)


@app.route('/translate', methods=['GET', 'POST'])
def translate():
    if flask.request.method == 'GET':
        sql = flask.request.args.get('q')
    else:
        sql = flask.request.get_data(parse_form_data=False)
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
    if flask.request.method == 'GET':
        sql = flask.request.args.get('q')
    else:
        sql = flask.request.get_data(parse_form_data=False)
    es_hosts = flask.request.args.get('elasticsearch')
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


@app.route('/search_with_arguments', methods=['POST'])
def search_with_arguments():
    req = json.loads(flask.request.get_data(parse_form_data=False))
    try:
        resp = {
            'error': None,
            'data': es_query.execute_sql(req['elasticsearch'], req['sql'], req['arguments'])
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
