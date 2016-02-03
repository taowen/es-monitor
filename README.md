## 插件说明

第一个参数是 odin 曲线的名字

第二个参数是 sql 比如

```
./es_monitor.py gs_plutus_debug.count "select count(*) as value from gs_plutus_debug where \"timestamp\" > @now-5m"
```

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python es_query.py
SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_ GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```
## 特殊语法

* ```@now`` 表示当前时间，可以加减s,m,h,d
* case when 表达 range aggregation ```
select fp, count(*) from gs_plutus_debug_
    where "timestamp">@now-15m group by (case when "timestamp" >= (@now-50s) and "timestamp" < (@now+50s) then 'future'
    when "timestamp" < (@now-50s) then 'now' end) as fp```
* date_trunc 表达 date histogram aggregation ```
select per_minute, count(*) from gs_plutus_debug_
    where "timestamp">@now-5m group by to_char(date_trunc('minute', "timestamp"),'yyyy-MM-dd HH:mm:ss') as per_minute```
* 在sql后面对结果进行python脚本后处理 ```
select eval("output['errno']=input.get('errno')") from (
    select * from gs_plutus_debug limit 1)```

## 注意

由于 odin-agent 限制特殊字符 `>`, 因此我们使用中文`》` 

TODO

* ``` SELECT user, MAX(value) FROM (SELECT user, COUNT(*) AS value FROM index GROUP BY user)```
* aggregation & sort & limit
* client side join
