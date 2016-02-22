## 插件说明

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

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python es_query.py http://es_hosts
    SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_
    GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```

具体SQL语法支持地程度，请阅读：https://segmentfault.com/a/1190000003502849

# Full text queries

## Match Query

TODO

## Multi Match Query

TODO

## Common Terms Query

TODO

## Query String Query

TODO

## Simple Query String Query

TODO

# Term level queries

## Term Query

```
{
    "term" : { "user" : "Kimchy" }
}
```

```
WHERE user='Kimchy'
```

If field is analyzed, term query actually means contains instead of fully equal

## Terms Query

TODO

## Range Query

```
{
    "range" : {
        "age" : {
            "gte" : 10,
            "lte" : 20
        }
    }
}
```

```
WHERE age >= 10 AND age <=  20
```

```
{
    "range" : {
        "date" : {
            "gte" : "now-1d",
            "lt" :  "now"
        }
    }
}
```

```
WHERE "date" >= now() - INTERVAL '1 day' AND "date" < now()
```