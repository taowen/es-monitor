# Installation

pip install es-sql

# Usage

```
import es_sql
es_sql.execute_sql(
    'http://127.0.0.1:9200',
    'SELECT COUNT(*) FROM your_index WHERE field=%(param)s',
    arguments={'param': 'value'})
```

arguments is optional if no %(param)s specified in the sql

```es-sql``` command can also be used in commandline:

```
cat << EOF | es-sql http://127.0.0.1:9200
    SELECT COUNT(*) FROM your_index
EOF
```

# Syntax

The goal is to be able to express all the necessary elasticsearch DSL
(used in the context of OLAP database, not full-text search engine) using SQL.

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
WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol);
WITH per_ipo_year AS (SELECT ipo_year, MAX(market_cap) AS max_this_year INSIDE all_symbols
    GROUP BY ipo_year LIMIT 2);
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
WITH finance_symbols AS (SELECT symbol FROM symbol WHERE sector='Finance' LIMIT 5);
SELECT MAX(adj_close) FROM quote
    JOIN finance_symbols ON quote.symbol = finance_symbols.symbol;
```

## Pagination

TODO

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

```
{
    "constant_score" : {
        "filter" : {
            "terms" : { "user" : ["kimchy", "elasticsearch"]}
        }
    }
}
```
```
WHERE user IN ('kimchy', 'elasticsearch')
```

Terms look up will not be supported, use server side join instead.

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
```
WHERE "date" >= today() - interval('1 day') AND "date" < today()
```
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
Suported datetime function are

- datetime: TIMESTAMP '2012-01-01 00:00:00' can also be timestamp('2012-01-01 00:00:00')
- day/hour/minute/second interval: INTERVAL '1 DAY' can also be interval('1 day')
- current datetime: now()
- current day: today()

TODO: timezone

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

```
{
    "type" : {
        "value" : "my_type"
    }
}
```
```
WHERE _type='my_type'
```

## Ids Query

```
{
    "ids" : {
        "values" : ["1", "4", "100"]
    }
}
```
```
WHERE _id IN ('1','4','100')
```
```
{
    "ids" : {
        "type" : "my_type",
        "values" : ["1", "4", "100"]
    }
}
```
```
WHERE _type='my_type' AND _id IN ('1','4','100')
```

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

## Indicies Query

TODO

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

```
{
    "aggs" : {
        "grades_stats" : { "extended_stats" : { "field" : "grade" } }
    }
}
```
will return
```
{
    "grade_stats": {
       "count": 9,
       "min": 72,
       "max": 99,
       "avg": 86,
       "sum": 774,
       "sum_of_squares": 67028,
       "variance": 51.55555555555556,
       "std_deviation": 7.180219742846005,
       "std_deviation_bounds": {
        "upper": 100.36043948569201,
        "lower": 71.63956051430799
       }
    }
}
```
```
SELECT SUM_OF_SQUARES(grade)
SELECT VARIANCE(grade)
SELECT STD_DEVIATION(grade)
SELECT STD_DEVIATION_UPPER_BOUND(grade)
SELECT STD_DEVIATION_LOWER_BOUND(grade)
```

TODO: script, missing

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

## Min Aggregation

```
{
    "aggs" : {
        "min_price" : { "min" : { "field" : "price" } }
    }
}
```
```
SELECT MIN(price) AS min_price
```

TODO: script, missing

## Percentiles Aggregation

TODO

## Percentile Ranks Aggregation

TODO

## Scripted Metric Aggregation

TODO

## Sum Aggregation

```
{
    "aggs" : {
        "intraday_return" : { "sum" : { "field" : "change" } }
    }
}
```
```
SELECT SUM(change) AS intraday_return
```

TODO: script, missing

## Top hits Aggregation

TODO

## Value Count Aggregation

```
{
    "aggs" : {
        "grades_count" : { "value_count" : { "field" : "grade" } }
    }
}
```
```
SELECT COUNT(grade) AS grades_count
```

TODO: script

# Bucket Aggregations

## Children Aggregation

TODO

## Date Historgram Aggregation

```
{
    "aggs" : {
        "articles_over_time" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            }
        }
    }
}
```
```
GROUP BY DATE_TRUNC('month', "date") AS articles_over_time
```
```
{
    "aggs" : {
        "articles_over_time" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "1M",
                "format" : "yyyy-MM-dd"
            }
        }
    }
}
```
```
GROUP BY TO_CHAR(DATE_TRUNC('month', "date"),'%Y-%m-%d') AS articles_over_time
```

TODO: 1.5 hours interval, timezone, offset, script, missing

## Filter Aggregation

```
{
    "aggs" : {
        "red_products" : {
            "filter" : { "term": { "color": "red" } },
            "aggs" : {
                "avg_price" : { "avg" : { "field" : "price" } }
            }
        }
    }
}
```
```
WITH all_products AS (SELECT COUNT(*) FROM product);
SELECT AVG(price) AS avg_price FROM all_products WHERE color='red';
```

If from table is not another named sql, the where condition will be translated to query instead of filter aggregation.

## Filters Aggregation

```
{
  "aggs" : {
    "messages" : {
      "filters" : {
        "other_bucket_key": "other_messages",
        "filters" : {
          "errors" :   { "term" : { "body" : "error"   }},
          "warnings" : { "term" : { "body" : "warning" }}
        }
      }
    }
  }
}
```
```
GROUP BY CASE WHEN body='error' THEN 'errors' WHEN body='warning' THEN 'warnings' ELSE 'other_messages' END AS messages
```

## Geo Distance Aggregation

TODO

## GeoHash grid Aggregation

TODO

## Histogram Aggregation

```
{
    "aggs" : {
        "prices" : {
            "histogram" : {
                "field" : "price",
                "interval" : 50
            }
        }
    }
}
```
```
GROUP BY histogram(price, 50) AS prices
```
```
{
    "aggs" : {
        "prices" : {
            "histogram" : {
                "field" : "price",
                "interval" : 50,
                "order" : { "_key" : "desc" }
            }
        }
    }
}
```
```
GROUP BY histogram(price, 50) AS prices ORDER BY prices DESC
```

TODO: min_doc_count, offset, buckets_path, missing

## IPv4 Range Aggregation

TODO

## Missing Aggregation

TODO

## Nested Aggregation

TODO

## Range Aggregation

```
{
    "aggs" : {
        "price_ranges" : {
            "range" : {
                "field" : "price",
                "ranges" : [
                    { "to" : 50 },
                    { "from" : 50, "to" : 100 },
                    { "from" : 100 }
                ]
            }
        }
    }
}
```
```
GROUP BY CASE
    WEHN price < 50 THEN 'range1'
    WHEN price >= 50 AND price < 100 THEN 'range2'
    WHEN price >= 100 THEN 'range3'
END AS price_ranges
```

TODO: script

## Reverse nested Aggregation

TODO

## Sampler Aggregation

TODO

## Significant Terms Aggregation

TODO

## Terms Aggregation

```
{
    "aggs" : {
        "genders" : {
            "terms" : { "field" : "gender" }
        }
    }
}
```
```
GROUOP BY gender AS genders
```
```
{
    "aggs" : {
        "products" : {
            "terms" : {
                "field" : "product",
                "size" : 5
            }
        }
    }
}
```
```
GROUP BY product AS products LIMIT 5
```
```
{
    "aggs" : {
        "genders" : {
            "terms" : {
                "field" : "gender",
                "order" : { "_count" : "asc" }
            }
        }
    }
}
```
```
SELECT COUNT(*) AS c FROM xxx
    GROUP BY gender AS genders ORDER BY c
```
```
{
    "aggs" : {
        "genders" : {
            "terms" : {
                "field" : "gender",
                "order" : { "height_stats.std_deviation" : "desc" }
            },
            "aggs" : {
                "height_stats" : { "extended_stats" : { "field" : "height" } }
            }
        }
    }
}
```
```
SELECT STD_DEVIATION(height) AS s FROM xxx
    GROUP BY gender AS genders ORDER BY s
```
```
{
    "aggs" : {
        "countries" : {
            "terms" : {
                "field" : "address.country",
                "order" : { "females>height_stats.avg" : "desc" }
            },
            "aggs" : {
                "females" : {
                    "filter" : { "term" : { "gender" :  "female" }},
                    "aggs" : {
                        "avg_height" : { "avg" : { "field" : "height" }}
                    }
                }
            }
        }
    }
}
```
```
WITH all AS (SELECT * FROM xxx GROUP BY address.country AS countries ORDER BY female_avg_height);
SELECT AVG(height) AS female_avg_height FROM all WHERE gender='female'
```

TODO: document count error, min_doc_count, script, filtering, collect-to, missing

# Pipeline Aggregations

## Avg Bucket Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                }
            }
        },
        "avg_monthly_sales": {
            "avg_bucket": {
                "buckets_path": "sales_per_month>sales"
            }
        }
    }
}
```
```
WITH sales_per_month AS (SELECT month, SUM(price) AS sales FROM sale GROUP BY DATE_TRUNC('month', "date") AS month);
SELECT AVG(sales) AS avg_monthly_sales FROM sales_per_month;
```

TODO: gap_policy

## Derivative Aggregation

First Order Derivative
```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                },
                "sales_deriv": {
                    "derivative": {
                        "buckets_path": "sales"
                    }
                }
            }
        }
    }
}
```
```
SELECT month, SUM(price) AS sales, DERIVATIVE(sales) AS sales_deriv
    FROM sale GROUP BY DATE_TRUNC('month', "date") AS month
```
Second Order Derivative
```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                },
                "sales_deriv": {
                    "derivative": {
                        "buckets_path": "sales"
                    }
                },
                "sales_2nd_deriv": {
                    "derivative": {
                        "buckets_path": "sales_deriv"
                    }
                }
            }
        }
    }
}
```
```
SELECT month, SUM(price) AS sales, DERIVATIVE(sales) AS sales_deriv, DERIVATIVE(sales_deriv) AS sales_2nd_deriv
    FROM sale GROUP BY DATE_TRUNC('month', "date") AS month
```

TODO: unit, gap_policy

## Max Bucket Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                }
            }
        },
        "max_monthly_sales": {
            "max_bucket": {
                "buckets_path": "sales_per_month>sales"
            }
        }
    }
}
```
```
WITH sales_per_month AS (SELECT month, SUM(price) AS sales FROM sale GROUP BY DATE_TRUNC('month', "date") AS month);
SELECT MAX(sales) AS max_monthly_sales FROM sales_per_month;
```

TODO: gap_policy

## Min Bucket Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                }
            }
        },
        "min_monthly_sales": {
            "min_bucket": {
                "buckets_path": "sales_per_month>sales"
            }
        }
    }
}
```
```
WITH sales_per_month AS (SELECT month, SUM(price) AS sales FROM sale GROUP BY DATE_TRUNC('month', "date") AS month);
SELECT MIN(sales) AS min_monthly_sales FROM sales_per_month;
```

TODO: gap_policy

## Sum Bucket Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                }
            }
        },
        "sum_monthly_sales": {
            "sum_bucket": {
                "buckets_path": "sales_per_month>sales"
            }
        }
    }
}
```
```
WITH sales_per_month AS (SELECT month, SUM(price) AS sales FROM sale GROUP BY DATE_TRUNC('month', "date") AS month);
SELECT SUM(sales) AS sum_monthly_sales FROM sales_per_month;
```

TODO: gap_policy

## Stats Bucket Aggregation

TODO

## Extended Stats Bucket Aggregation

TODO

## Percentiles Bucket Aggregation

TODO

## Moving Average Aggregation

```
{
    "moving_avg": {
        "buckets_path": "the_sum",
        "model": "holt",
        "window": 5,
        "gap_policy": "insert_zero",
        "settings": {
            "alpha": 0.8
        }
    }
}
```
```
SELECT moving_avg(the_sum, '{"model":"holt","window":5,"gap_policy":"insert_zero","settings":{"alpha":0.8}}')
```
Can also be
```
SELECT moving_avg(the_sum, model='holt', window=5, gap_policy='insert_zero', settings='{"alpha":0.8}')
```

## Cumulative Sum Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "sales": {
                    "sum": {
                        "field": "price"
                    }
                },
                "cumulative_sales": {
                    "cumulative_sum": {
                        "buckets_path": "sales"
                    }
                }
            }
        }
    }
}
```
```
SELECT month, SUM(price) AS sales, CSUM(sales) AS cumulative_sales
    FROM sale GROUP BY DATE_TRUNC('month', "date") AS month
```

## Bucket Script Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "total_sales": {
                    "sum": {
                        "field": "price"
                    }
                },
                "t-shirts": {
                  "filter": {
                    "term": {
                      "type": "t-shirt"
                    }
                  },
                  "aggs": {
                    "sales": {
                      "sum": {
                        "field": "price"
                      }
                    }
                  }
                },
                "t-shirt-percentage": {
                    "bucket_script": {
                        "buckets_path": {
                          "tShirtSales": "t-shirts>sales",
                          "totalSales": "total_sales"
                        },
                        "script": "tShirtSales / totalSales * 100"
                    }
                }
            }
        }
    }
}
```
```
WITH sales_per_month AS (
    SELECT month, SUM(price) AS total_sales, tshirt_sales/total_sales AS t-shirt-percentage
    FROM sale GROUP BY DATE_TRUNC('month', "date") AS month);
SELECT SUM(price) AS tshirt_sales FROM sales_per_month WHERE type='t-shirt';
```

## Bucket Selector Aggregation

```
{
    "aggs" : {
        "sales_per_month" : {
            "date_histogram" : {
                "field" : "date",
                "interval" : "month"
            },
            "aggs": {
                "total_sales": {
                    "sum": {
                        "field": "price"
                    }
                }
                "sales_bucket_filter": {
                    "bucket_selector": {
                        "buckets_path": {
                          "totalSales": "total_sales"
                        },
                        "script": "totalSales <= 50"
                    }
                }
            }
        }
    }
}
```
```
SELECT month, SUM(price) AS total_sales
    FROM sale GROUP BY DATE_TRUNC('month', "date") AS month
    HAVING total_sales <= 50
```

TODO: gap_policy

## Serial Differencing Aggregation

```
{
   "aggs": {
      "my_date_histo": {
         "date_histogram": {
            "field": "timestamp",
            "interval": "day"
         },
         "aggs": {
            "the_sum": {
               "sum": {
                  "field": "lemmings"
               }
            },
            "thirtieth_difference": {
               "serial_diff": {
                  "buckets_path": "the_sum",
                  "lag" : 30
               }
            }
         }
      }
   }
}
```
```
SELECT SUM(lemmings) AS the_sum, SERIAL_DIFF(the_sum, lag=30) AS thirtieth_difference FROM xxx
    GROUP BY DATE_TRUNC('day', "timestamp") AS my_date_histo
```