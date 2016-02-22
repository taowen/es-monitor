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

# 命令行 SQL 查询 Elasticsearch

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python es_query.py http://es_hosts
    SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_
    GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```

教程请阅读：https://segmentfault.com/a/1190000003502849

# Syntax

## Query multiple index

```FROM quote``` => ```quote*```

```FROM index('quote')``` => ```quote```

```FROM index('quote-%Y-%m-%d', '2015-01-01')``` => ```quote-2015-01-01```

```FROM index('quote-%Y-%m-%d', '2015-01-01', '2015-01-03')``` => ```quote-2015-01-01,quote-2015-01-02,quote-2015-01-03```

```FROM index('quote-%Y-%m-%d', now())```

```FROM index('quote-%Y-%m-%d', now() - interval('2 DAYS'))```

```FROM (index('quote') UNION index('symbol')) AS my_table``` => ```quote,symbol```

```FROM (quote EXCEPT index('quote-2015-01-01')) AS my_table``` => ```quote*,-quote-2015-01-01```

## Drill down by sub aggregation

Elasticsearch support sub aggregations. It can be expressed by multiple sql statements

```
WITH SELECT MAX(market_cap) AS max_all_times FROM symbol AS all_symbols;
WITH SELECT ipo_year, MAX(market_cap) AS max_this_year INSIDE all_symbols
    GROUP BY ipo_year LIMIT 2 AS per_ipo_year;
```

```SELECT INSIDE``` can also be ```SELECT FROM```

## Client side join

```
SELECT symbol FROM symbol WHERE sector='Finance' LIMIT 5;
SAVE RESULT AS finance_symbols;
SELECT MAX(adj_close) FROM quote
    JOIN finance_symbols ON quote.symbol = finance_symbols.symbol;
REMOVE RESULT finance_symbols;
```

## Server side join

It requires https://github.com/sirensolutions/siren-join

```
WITH SELECT symbol FROM symbol WHERE sector='Finance' LIMIT 5 AS finance_symbols;
SELECT MAX(adj_close) FROM quote
    JOIN finance_symbols ON quote.symbol = finance_symbols.symbol;
```

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

```
{
    "range" : {
        "date" : {
            "gte" : "now-1d/d",
            "lt" :  "now/d"
        }
    }
}
```
TODO
```
{
    "range" : {
        "born" : {
            "gte": "01/01/2012",
            "lte": "2013",
            "format": "dd/MM/yyyy||yyyy"
        }
    }
}
```
```
WHERE born >= TIMESTAMP '2012-01-01 00:00:00' AND born <= TIMESTAMP '2013-01-01 00：00：00'
```

## Exists Query

```
{
    "exists" : { "field" : "user" }
}
```
```
WHERE user IS NOT NULL
```

## Prefix Query

TODO

## Wildcard Query

```
{
    "wildcard" : { "user" : "ki*y" }
}
```
```
WHERE user LIKE 'ki%y'
```

```
{
    "wildcard" : { "user" : "ki?y" }
}
```
```
WHERE user LIKE 'ki_y'
```

## Regexp Query

TODO

## Fuzzy Query

TODO

## Type Query

TODO

## Ids Query

TODO

# Compound queries

## Bool Query

```
{
    "bool" : {
        "must" : {
            "term" : { "user" : "kimchy" }
        },
        "filter": {
            "term" : { "tag" : "tech" }
        },
        "must_not" : {
            "range" : {
                "age" : { "from" : 10, "to" : 20 }
            }
        },
        "should" : [
            {
                "term" : { "tag" : "wow" }
            },
            {
                "term" : { "tag" : "elasticsearch" }
            }
        ]
    }
}
```
```
WHERE user='kimchy' AND tag='tech' AND NOT (age >= 10 AND age < 20) AND (tag='wow' OR tag='elasticsearch')
```

TODO: minimum_should_match

## Limit Query

TODO

# Joining queries

## Nested Query

TODO

## Has Child Query

TODO

## Has Parent Query

TODO

# Geo queries

## GeoShape Query

TODO

## Geo Bounding Box Query

TODO

## Geo Distance Query

TODO

## Geo Distance Range Query

TODO

## Geo Polygon Query

TODO

## Geohash Cell Query

TODO

# Specialized queries

## Template Query

TODO

## Script Query

TODO

# Metric Aggregations

## Avg Aggregation

```
{
    "aggs" : {
        "avg_grade" : { "avg" : { "field" : "grade" } }
    }
}
```
```
SELECT avg(grade) AS avg_grade
```

TODO: script, missing

## Cardinality Aggregation

```
{
    "aggs" : {
        "author_count" : {
            "cardinality" : {
                "field" : "author"
            }
        }
    }
}
```
```
SELECT COUNT(DISTINCT author) AS author_count
```
TODO: Precision control, script, missing

## Extended Stats Aggregation

TODO

## Geo Bounds Aggregation

TODO

## Geo Centroid Aggregation

TODO

## Max Aggregation

```
{
    "aggs" : {
        "max_price" : { "max" : { "field" : "price" } }
    }
}
```
```
SELECT MAC(price) AS max_price
```

TODO: script, missing
