## 插件说明

```
./plugin.sh https://url-to-params
```

从 url 处下载的文件内容：

* 第一行参数是 elasticsearch 服务器地址
* 后面是 sql
* sql 后面是 >>> 曲线名称

比如

```
http://es_hosts

select count(*) as value from gs_plutus_debug
where "timestamp" > @now-5m
>>> gs_plutus_debug.count

select count(*) as value from gs_api_track
where "@timestamp" > @now-5m
>>> gs_api_track.count
```

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python es_query.py http://es_hosts
SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_ GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```
## 计算后再聚合

* ```@now`` 表示当前时间，可以加减s,m,h,d
* case when 表达 range aggregation（不支持else，只支持>=和<） ```
select fp, count(*) from gs_plutus_debug_
    where "timestamp">@now-15m group by (case when "timestamp" >= (@now-50s) and "timestamp" < (@now+50s) then 'future'
    when "timestamp" < (@now-50s) then 'now' end) as fp```
* case when 表达 filters aggregation （支持else，任意表达式） ```
select status, count(*) as value from gs_plutus_debug
    group by (case when status='200' then 'success' else 'failure' end) as status
    where "timestamp">@now-5h and name='getEstimatePrice'```
* date_trunc 表达 date histogram aggregation ```
select per_minute, count(*) from gs_plutus_debug_
    where "timestamp">@now-5m group by to_char(date_trunc('minute', "timestamp"),'yyyy-MM-dd HH:mm:ss') as per_minute```


## 嵌套下钻

* Elasticsearch 支持对一次聚合的结果进行下钻获取细分的聚合。所以扩充了SQL的语法支持了SELECT INSIDE，比如 ```
select count(*) as success inside (select count(*) as total
from "gs_plutus_debug_2016-02-05" where "timestamp" > @now-5m) where errno=0```

## 嵌套子查询

* 支持 Having 对聚合的结果进行二次过滤 ```
select count(*) as total_count from gs_api_track
    where "@timestamp" > @now-10s group by date_trunc('second', "@timestamp") as ts
    having total_count>10
```

## 用python进行后处理

* 在sql后面对结果进行python脚本后处理（逐行） ```
select eval("output['errno']=input.get('errno')") from (
    select * from gs_plutus_debug limit 1)```
* 行变列 ```
select pivot(errno, value) from (
    select errno, count(*) as value from gs_plutus_debug where "timestamp" > @now-5m group by errno)``` 是把这样的输出 ```
{"errno": 0, "value": 234171}
{"errno": 97, "value": 76}
``` 变成这样 ```
{"errno_0": 234171, "errno_97": 76}
```

## 减少不同的写法

* 不支持 a NOT IN b, a IS NOT NULL 使用 NOT a IN b 替代
* 如果使用了函数必须有AS，引用函数计算的值用AS的变量名替代

TODO

* pull up select inside executor
* select from per bucket
* support bucket script
* support parent pipeline aggregation at metric level: derivative, difference, moving average, cumulative sum
* support output=None, support inputs[-1]
* in memory computation support where, group by
* histogram aggregation
* client side join
