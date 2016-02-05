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

## 聚合后二次计算

* 普通的SQL子查询聚合越聚合越少，但是Elasticsearch的嵌套聚合是越聚合越多。上一层从下一层的结果里进一步
group by（或者过滤）是表示对数据的进一步细分，从语义上来说和SQL是相反的。```
select count(*) as sub_count from (
    select count(*) as total_count from gs_api_track where "@timestamp" > @now-10s
    group by date_trunc('second', "@timestamp") as ts) where "order.district"='010'``` 返回的结果类似这样 ```
{"total_count": 13, "sub_count": 1, "ts": "2016-02-05T00:16:16.000+08:00"}
{"total_count": 7, "sub_count": 5, "ts": "2016-02-05T00:16:17.000+08:00"}
{"total_count": 0, "sub_count": 0, "ts": "2016-02-05T00:16:18.000+08:00"}
{"total_count": 0, "sub_count": 0, "ts": "2016-02-05T00:16:19.000+08:00"}
{"total_count": 0, "sub_count": 0, "ts": "2016-02-05T00:16:20.000+08:00"}
{"total_count": 88, "sub_count": 17, "ts": "2016-02-05T00:16:21.000+08:00"}
{"total_count": 9, "sub_count": 2, "ts": "2016-02-05T00:16:22.000+08:00"}
{"total_count": 5, "sub_count": 1, "ts": "2016-02-05T00:16:23.000+08:00"}
{"total_count": 4, "sub_count": 1, "ts": "2016-02-05T00:16:24.000+08:00"}```
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

* support output=None, support inputs[-1]
* support SELECT * INSIDE ()
* support parent pipeline aggregation at metric level: derivative, difference, moving average, cumulative sum
* in memory computation support where, group by
* implement nested where to replace having
* support cross layer having
* support bucket script
* histogram aggregation
* ``` SELECT user, MAX(value) FROM (SELECT user, COUNT(*) AS value FROM index GROUP BY user)```
* client side join
