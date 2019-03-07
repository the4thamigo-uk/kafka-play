# Join to latest

## Introduction

The following, is a discussion of how to join two streams, such that, each left stream record joins to only the most recent record in the right stream.

This performs a similar operation found in other databases e.g. the `fill` specifier of the `group by` [clause in influxql](https://docs.influxdata.com/influxdb/v1.7/query_language/data_exploration/#basic-group-by-time-syntax).

By default, kafka [stream-stream joins](https://docs.confluent.io/current/ksql/docs/developer-guide/join-streams-and-tables.html#semantics-of-stream-stream-joins) perform windowed joins which join all possible records within the window (which moves over time). Furthermore, the window extends into the future and into the past, so the join cannot be restricted to only consider past right records. So there is no way to do this out-of-the-box it seems.

The stream equivalent is particularly tricky, because it has to deal with late and out-of-order data, so that groups of left records need to be re-calculated when right records arrive late, and vice-versa.

Also, in order to handle late-arriving data, the backlog of both streams needs to be cached, and therefore for high-frequency data or data that can arrive _very_ late, you might need to cache a significant amount of data. In Kafka streams, a [retention period](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters) is set for the records in the join windows, and any late records that arrive for times prior to this retention period are simply [discarded](https://docs.confluent.io/current/ksql/docs/developer-guide/join-streams-and-tables.html#joins-and-windows), leading to missing and potentially inaccurate data.

There are several ways you might have late arriving data, for example :

1) Unreliable or slow transmission of data
2) Manually entered data, including corrections to previously incorrectly entered data
3) Reingestion of archive data - e.g. a section of historical data was either missing or innaccurate in the initial ingestion and needs to be patched. 
4) Some stream calculation logic has been changed - though in this case, rather than reingesting the data at source, it is more appropriate to recalculate from the start of the original unmodified stream(s).  

## Experiment with KSQL Joins

Assuming the stream `x` is our left stream and `y` is our right stream. We can perform a KSQL join operation on any key field e.g. `key2`:

```
ksql> select * from x inner join y within 5 seconds on x.key2 = y.key2;
0 | k2 | 0 | k1 | k2 | 0 | 0 | k2 | 0 | k1 | k2 | 10
0 | k2 | 0 | k1 | k2 | 0 | 5000 | k2 | 5000 | k1 | k2 | 50
1000 | k2 | 1000 | k1 | k2 | 1 | 0 | k2 | 0 | k1 | k2 | 10
1000 | k2 | 1000 | k1 | k2 | 1 | 5000 | k2 | 5000 | k1 | k2 | 50
2000 | k2 | 2000 | k1 | k2 | 2 | 0 | k2 | 0 | k1 | k2 | 10
2000 | k2 | 2000 | k1 | k2 | 2 | 5000 | k2 | 5000 | k1 | k2 | 50
3000 | k2 | 3000 | k1 | k2 | 3 | 0 | k2 | 0 | k1 | k2 | 10
3000 | k2 | 3000 | k1 | k2 | 3 | 5000 | k2 | 5000 | k1 | k2 | 50
4000 | k2 | 4000 | k1 | k2 | 4 | 0 | k2 | 0 | k1 | k2 | 10
4000 | k2 | 4000 | k1 | k2 | 4 | 5000 | k2 | 5000 | k1 | k2 | 50
5000 | k2 | 5000 | k1 | k2 | 5 | 0 | k2 | 0 | k1 | k2 | 10
5000 | k2 | 5000 | k1 | k2 | 5 | 5000 | k2 | 5000 | k1 | k2 | 50
6000 | k2 | 6000 | k1 | k2 | 6 | 5000 | k2 | 5000 | k1 | k2 | 50
7000 | k2 | 7000 | k1 | k2 | 7 | 5000 | k2 | 5000 | k1 | k2 | 50
8000 | k2 | 8000 | k1 | k2 | 8 | 5000 | k2 | 5000 | k1 | k2 | 50
9000 | k2 | 9000 | k1 | k2 | 9 | 5000 | k2 | 5000 | k1 | k2 | 50
10000 | k2 | 10000 | k1 | k2 | 10 | 5000 | k2 | 5000 | k1 | k2 | 50
```

Note: the ROWKEY must be set during ingestion of the data to the source topic, for the join to return any results. This is because joins require both streams to be partitioned in the same way using the same ROWKEY, and a separate join window is maintained for each value of the ROWKEY (see 'windows are tracked per record key [here](https://docs.confluent.io/current/ksql/docs/developer-guide/join-streams-and-tables.html#joins-and-windows).

As mentioned above, the join window of 5 seconds extends into the future as well as into the past. So the x record at event_time = 1000 joins to both y records at event_time = 0 and event_time = 5000. 

To limit this window to the past we can add a `where` clause such as :

```
ksql> select * from x inner join y within 5 seconds on x.key2 = y.key2 where x.event_time >= y.event_time;
0 | k2 | 0 | k1 | k2 | 0 | 0 | k2 | 0 | k1 | k2 | 10
1000 | k2 | 1000 | k1 | k2 | 1 | 0 | k2 | 0 | k1 | k2 | 10
2000 | k2 | 2000 | k1 | k2 | 2 | 0 | k2 | 0 | k1 | k2 | 10
3000 | k2 | 3000 | k1 | k2 | 3 | 0 | k2 | 0 | k1 | k2 | 10
4000 | k2 | 4000 | k1 | k2 | 4 | 0 | k2 | 0 | k1 | k2 | 10
5000 | k2 | 5000 | k1 | k2 | 5 | 0 | k2 | 0 | k1 | k2 | 10
5000 | k2 | 5000 | k1 | k2 | 5 | 5000 | k2 | 5000 | k1 | k2 | 50
6000 | k2 | 6000 | k1 | k2 | 6 | 5000 | k2 | 5000 | k1 | k2 | 50
7000 | k2 | 7000 | k1 | k2 | 7 | 5000 | k2 | 5000 | k1 | k2 | 50
8000 | k2 | 8000 | k1 | k2 | 8 | 5000 | k2 | 5000 | k1 | k2 | 50
9000 | k2 | 9000 | k1 | k2 | 9 | 5000 | k2 | 5000 | k1 | k2 | 50
10000 | k2 | 10000 | k1 | k2 | 10 | 5000 | k2 | 5000 | k1 | k2 | 50
```

Note: it seems that the duplicate row for 5000 is caused by first considering records where `x.event_time <= 5000` and `y.event_time < 5000`,
then `x.event_time <= 5000` and `y.event_time <= 5000`.

And then we can use `group by` to compute aggregates for each event_time and join key :

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x inner join y within 5 seconds on x.key2 = y.key2 where x.event_time >= y.event_time group by x.event_time, x.key2;
0 | k2 | 0 | 10
1000 | k2 | 1 | 10
2000 | k2 | 2 | 10
3000 | k2 | 3 | 10
4000 | k2 | 4 | 10
5000 | k2 | 5 | 10
5000 | k2 | 5 | 50
6000 | k2 | 6 | 50
7000 | k2 | 7 | 50
8000 | k2 | 8 | 50
9000 | k2 | 9 | 50
10000 | k2 | 10 | 50
```

Note: this is an unwindowed aggregation which means it is an aggregation over the entire stream of data.
Note: the duplicate row for `x.event_time = 5000` persists into this grouped query, but the last event in the changelog wins.

This gives us _something like_ a join of each x record to the most recent y record. Its not quite right because we are using `max` and it only looks like it is working because we have ingested data values that increase as event_time increases. But, we _could potentially_ write a custom [UDAF](https://www.confluent.io/blog/build-udf-udaf-ksql-5-0) to return the latest data within the group for each field.
One possible problem with this approach is that it relies on the records within each group to be ordered by the y.event_time, because in a [UDAF](https://docs.confluent.io/current/ksql/docs/developer-guide/udf.html#example-udaf-class), although you can maintain state as you aggregate/reduce, you do not have access to the entire row of data, so you cannot observe the y.event_time.

https://github.com/confluentinc/ksql/issues/1128
https://github.com/confluentinc/ksql/issues/1373

Alternatively, it looks like there is a pure KSQL approach using the `collect_list` aggregate function, though it is messy :

```
ksql> select x.event_time, x.key2, collect_list(x.val)[cast (count(*) as int)-1], collect_list(y.val)[cast (count(*) as int)-1] from x inner join y within 5 seconds on x.key2 = y.key2 where x.event_time >= y.event_time group by x.event_time, x.key2;
0 | k2 | 0 | 10
1000 | k2 | 1 | 10
2000 | k2 | 2 | 10
3000 | k2 | 3 | 10
4000 | k2 | 4 | 10
5000 | k2 | 5 | 10
5000 | k2 | 5 | 50
6000 | k2 | 6 | 50
7000 | k2 | 7 | 50
8000 | k2 | 8 | 50
9000 | k2 | 9 | 50
10000 | k2 | 10 | 50
```

However, there are other problems with the above query, specifically when there are x records that dont have any y records present in the join window.
We simulate this situation, by reducing the size of the join window to 1 second :

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x inner join y within 1 seconds on x.key2 = y.key2 where x.event_time >= y.event_time group by x.event_time, x.key2;
0 | k2 | 0 | 10
1000 | k2 | 1 | 10
5000 | k2 | 5 | 50
6000 | k2 | 6 | 50
```

Here, we have missing x records, because there were no y candidates to join to. We can fix this with a left join:

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
0 | k2 | 0 | -2147483648
0 | k2 | 0 | 10
1000 | k2 | 1 | 10
2000 | k2 | 2 | -2147483648
3000 | k2 | 3 | -2147483648
4000 | k2 | 4 | -2147483648
5000 | k2 | 5 | 50
6000 | k2 | 6 | 50
7000 | k2 | 7 | -2147483648
8000 | k2 | 8 | -2147483648
9000 | k2 | 9 | -2147483648
10000 | k2 | 10 | -2147483648
11000 | k2 | 11 | -2147483648
12000 | k2 | 12 | -2147483648
13000 | k2 | 13 | -2147483648
```

If we assume we can fix `max` so that it better handles `null` (so it doesnt emit LONG_MIN), then this gives something better.

Specifically, the query above now _always_ returns our left record and joins to the _latest_ right record when there is something to join to within the _past_ window.

Now, we can simulate some late and out-of-order data for y. Lets assume we get a y record at y.event_time = 3000 :

```
echo -e '"rk1"\t{ "event_time": 3000, "key1": "k1", "key2": "k2", "val": 30 }' | ./produce.sh y
```

Now re-run the previous join, and we have two new records on the end :

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
...
3000 | k2 | 3 | 30
4000 | k2 | 4 | 30
```

This is expected, and correct. the x records at 3000 and 4000 have been updated with the new y record that lies within their join window.

Lets now assume there is more late data:

```
echo -e '"rk1"\t{ "event_time": 4000, "key1": "k1", "key2": "k2", "val": 40 }' | ./produce.sh y
```

And we see that both the x record at 4000 is correctly modified, and the record at 5000 is correctly re-calculated as unchanged.  

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
...
4000 | k2 | 4 | 40
5000 | k2 | 5 | 50
```

We can also receive late and out-of-order data for x :

```
echo -e '"rk1"\t{ "event_time": 4500, "key1": "k1", "key2": "k2", "val": 4.5 }' | ./produce.sh x
```

Which correctly gives us :

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
...
4500 | k2 | 4 | 40
```

What happens if there are two candidate y records in a given join window? Lets insert a y record at 4500, so that x at 5000 will group with y at 4500 and y at 5000:

```
echo -e '"rk1"\t{ "event_time": 4500, "key1": "k1", "key2": "k2", "val": 45 }' | ./produce.sh y
```

We correctly get recomputations :

```
ksql> select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
4500 | k2 | 4 | 45
5000 | k2 | 5 | 50
```

Ok, so looks good, so lets create a persistent stream :

```
ksql> create stream xy as select x.event_time, x.key2, max(x.val), max(y.val) from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
Invalid result type. Your SELECT query produces a TABLE. Please use CREATE TABLE AS SELECT statement instead.
```

So, instead we have to create a KTable:

```
ksql> create table xy as select x.event_time as event_time, x.key2 as key2, max(x.val) as x_val, max(y.val) as y_val from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;

 Message                   
---------------------------
 Table created and running 
---------------------------
```

Then we can simply listen for updates as a changelog:

```
ksql> select * from xy;
3000 | 3000|+|k2 | 3000 | k2 | 3 | -2147483648
3000 | 3000|+|k2 | 3000 | k2 | 3 | 30
7000 | 7000|+|k2 | 7000 | k2 | 7 | -2147483648
8000 | 8000|+|k2 | 8000 | k2 | 8 | -2147483648
4000 | 4000|+|k2 | 4000 | k2 | 4 | -2147483648
5000 | 5000|+|k2 | 5000 | k2 | 5 | 50
3000 | 4000|+|k2 | 4000 | k2 | 4 | 30
4000 | 4000|+|k2 | 4000 | k2 | 4 | 40
4000 | 5000|+|k2 | 5000 | k2 | 5 | 50
4500 | 5000|+|k2 | 5000 | k2 | 5 | 50
9000 | 9000|+|k2 | 9000 | k2 | 9 | -2147483648
11000 | 11000|+|k2 | 11000 | k2 | 11 | -2147483648
4500 | 4500|+|k2 | 4500 | k2 | 4 | 40
4500 | 4500|+|k2 | 4500 | k2 | 4 | 45
0 | 0|+|k2 | 0 | k2 | 0 | -2147483648
6000 | 6000|+|k2 | 6000 | k2 | 6 | 50
0 | 0|+|k2 | 0 | k2 | 0 | 10
1000 | 1000|+|k2 | 1000 | k2 | 1 | 10
2000 | 2000|+|k2 | 2000 | k2 | 2 | -2147483648
10000 | 10000|+|k2 | 10000 | k2 | 10 | -2147483648
12000 | 12000|+|k2 | 12000 | k2 | 12 | -2147483648
13000 | 13000|+|k2 | 13000 | k2 | 13 | -2147483648
```
Note: the stream records for the KTable have a strange order. Not quite sure why this is, but we assume that this materializes to the same results as above... needs more research to understand this...

Note also that the KTable rows are assigned compound ROWKEYs of the `group by` fields :

```
ksql> select ROWKEY from xy3;
6000|+|k2
4000|+|k2
5000|+|k2
4000|+|k2
4000|+|k2
5000|+|k2
5000|+|k2
5000|+|k2
9000|+|k2
11000|+|k2
4500|+|k2
4500|+|k2
0|+|k2
3000|+|k2
0|+|k2
1000|+|k2
2000|+|k2
10000|+|k2
12000|+|k2
13000|+|k2
3000|+|k2
7000|+|k2
8000|+|k2
```

So far so good, but the big downside to this approach is that the KTable is grouped by `event_time` and so it will grow over time without limit.
My understanding is that this means that the underlying RocksDB will swap to disk and eventually fill the disk up.

So, if we used this kind of join-and-aggregate approach, we would need to find [a way](https://docs.confluent.io/current/ksql/docs/developer-guide/aggregate-streaming-data.html#tombstone-records)
to drop old records. Since KSQL is built on top of the Streams API, then you would have 
the same issue.

A further issue is that _very_ late data (namely data that is received outside of the retention period.

Its also not clear how performant this is.

## Experiment with Stream Processors

Assumption is that reproducing the above join-and-aggregate logic would lead to the same problems, although it will probably give more flexibility if the UDAF approach does not work.

What is clear is that the naieve approach of maintaining the latest y record in memory and joining it to each x record as it arrives, clearly does not work for many reasons. At a minimum, you have to cater for late-arriving records, so you _must_ cache recent stream data for x and y, and perform lookups and recalculations when late records arrive. This hand-crafted logic would be very similar to what is being done in the windowed join operation, but we can optimise a little since we are doing a very specific kind of join :

When a late x record, X,  arrives :

1) search the y cache for the nearest record Y, such that y.event_time < x.event_time.
2) join X to Y and emit record to stream

When a late y record, Y, arrives :

1) search the y cache for the next y record such that y.event_time > Y.event_time. Call this YN.
2) search the x cache for all the x records where x.event_time >= Y.event_time and x.event_time < YN.event_time
3) for all the found x records, join each one to the Y record and emit a stream record for each

Also we need to be able to integrate with the kafka streams state store, probably using [WindowStore](https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/state/WindowStore.html), which has a convenient [fetch](https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/state/ReadOnlyWindowStore.html#fetch-K-K-long-long-) function.

TBD
