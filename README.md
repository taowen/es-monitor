[![Gitter](https://badges.gitter.im/taowen/es-monitor.svg)](https://gitter.im/taowen/es-monitor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

# 插件说明

```
./plugin.sh https://url-to-params
```

从 url 处下载的文件内容：

* 第一行参数是 elasticsearch 服务器地址
* 后面是 sql

比如

```
http://es_hosts

SELECT count(*) AS value FROM gs_plutus_debug
    WHERE "timestamp" > now() - INTERVAL '5 minutes';
SAVE RESULT AS gs_plutus_debug.count;

SELECT count(*) AS value FROM gs_api_track
    WHERE "@timestamp" > now() - INTERVAL '5 minutes';
SAVE RESULT AS gs_api_track.count;
```

# SQL 查询 Elasticsearch

教程请阅读：https://segmentfault.com/a/1190000003502849

## 命令行

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python -m elasticsearch_sql http://es_hosts
    SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_
    GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```

## HTTP

启动http服务器（gunicorn）
```
python -m explorer
```
翻译 SQL 为 Elasticsearch 查询
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

用SQL直接查询 Elasticsearch 取得结果
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

带参数查询
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