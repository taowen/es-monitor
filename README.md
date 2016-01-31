## 插件说明

第一个参数是 odin 曲线的名字

第二个参数是 sql 比如

```
python es_monitor.py series "SELECT \"user\", oid, max(\"@timestamp\") as value FROM gs_api_track_ GROUP BY \"user\", oid WHERE \"@timestamp\" > 1454239084000"
```

在命令行上测试的时候也可以用stdin传sql参数，比如

```
cat << EOF | python es_query.py
SELECT "user", "oid", max("@timestamp") as value FROM gs_api_track_ GROUP BY "user", "oid" WHERE "@timestamp" > 1454239084000
EOF
```