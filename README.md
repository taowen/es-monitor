[![Gitter](https://badges.gitter.im/taowen/es-monitor.svg)](https://gitter.im/taowen/es-monitor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

A set of tools to query Elasticsearch with SQL

Tutorial in Chinese: https://segmentfault.com/a/1190000003502849

# As Monitor Plugin

```
./plugin.sh https://url-to-params
```

The url points to a file with format：

* first line: elasticsearch http url
* remaining lines: sql

For example

```
http://es_hosts

SELECT count(*) AS value FROM gs_plutus_debug
    WHERE "timestamp" > now() - INTERVAL '5 minutes';
SAVE RESULT AS gs_plutus_debug.count;

SELECT count(*) AS value FROM gs_api_track
    WHERE "@timestamp" > now() - INTERVAL '5 minutes';
SAVE RESULT AS gs_api_track.count;
```

Basic authentication is supported

```
cat << EOF | python -m es_sql http://xxx:8000/
    VAR username=hello;
    VAR password=world;
    SELECT count(*) FROM my_index
EOF
```

Can also use SQL to query Elasticsearch cluster health stats
```
SELECT * FROM _cluster_health
SELECT * FROM _cluster_state
SELECT * FROM _cluster_stats
SELECT * FROM _cluster_pending_tasks
SELECT * FROM _cluster_reroute
SELECT * FROM _nodes_stats
SELECT * FROM _nodes_info
SELECT * FROM _indices_stats
SELECT * FROM _indices_stats.all
SELECT * FROM _indices_stats.[index_name]
```

The output will be a JSON array containing data points

# As Console Command

For example

```
cat << EOF | python -m es_sql http://es_hosts
    SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_
    GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```

```python -m es_sql``` can be ```es-sql``` if ```pip install es-sql```

# As Python Library

```
pip install es-sql
```
```
import es_sql
es_sql.execute_sql(
    'http://127.0.0.1:9200',
    'SELECT COUNT(*) FROM your_index WHERE field=%(param)s',
    arguments={'param': 'value'})
```
For more information: https://github.com/taowen/es-monitor/tree/master/es_sql

# As HTTP Api

Start http server (gunicorn)
```
python -m explorer
```
Translate SQL to Elasticsearch DSL request
```
$ cat << EOF | curl -X POST -d @- http://127.0.0.1:8000/translate
SELECT * FROM quote WHERE symbol='AAPL'
EOF

{
  "data": {
    "indices": "quote*",
    "query": {
      "term": {
        "symbol": "AAPL"
      }
    }
  },
  "error": null
}
```

Use SQL to query Elasticsearch
```
$ cat << EOF | curl -X POST -d @- http://127.0.0.1:8000/search?elasticsearch=http://127.0.0.1:9200
SELECT COUNT(*) FROM quote WHERE symbol='AAPL'
EOF

{
  "data": {
    "result": [
      {
        "COUNT(*)": 8790
      }
    ]
  },
  "error": null
}
```

Use SQL with arguments
```
$ cat << EOF | curl -X POST -d @- http://127.0.0.1:8000/search_with_arguments
{
    "elasticsearch":"http://127.0.0.1:9200",
    "sql":"SELECT COUNT(*) FROM quote WHERE symbol=%(param1)s",
    "arguments":{"param1":"AAPL"}
}
EOF
{
  "data": {
    "result": [
      {
        "COUNT(*)": 8790
      }
    ]
  },
  "error": null
}
```