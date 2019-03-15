# Join to latest

## Introduction

The following, is a discussion of how to join two streams, such that, each left stream record joins to only the nearest past record in the right stream.

This performs a similar operation found in other databases e.g. the `fill` specifier of the `group by` [clause in influxql](https://docs.influxdata.com/influxdb/v1.7/query_language/data_exploration/#basic-group-by-time-syntax).

By default, kafka [stream-stream joins](https://docs.confluent.io/current/ksql/docs/developer-guide/join-streams-and-tables.html#semantics-of-stream-stream-joins) perform windowed joins which join all possible records within the window (which moves over time). Furthermore, the window extends into the future and into the past, so the join cannot be restricted to only consider past right records. So there is no way to do this out-of-the-box it seems.

The stream equivalent is particularly tricky, because it has to deal with late-arriving and out-of-order records, so that groups of left records need to be re-calculated when right records arrive late, and vice-versa.

We make a distinction here :
- **late-arriving records** - this is a record that arrives for processing in order of event_time, but arrives for processing later than expected.
- **out-of-order records** - this is a record that arrives for processing out of order of event_time. By definition, since the latest event_time in the stream is after the event_time of the record, an out-of-order record is also late-arriving. 

Also, in order to handle late-arriving data, the backlog of both streams needs to be cached, and therefore for high-frequency data or data that can arrive _very_ late, you might need to cache a significant amount of data. In Kafka streams, a [retention period](https://docs.confluent.io/current/streams/developer-guide/config-streams.html#optional-configuration-parameters) is set for the records in the join windows, and any late records that arrive for times prior to this retention period are simply [discarded](https://docs.confluent.io/current/ksql/docs/developer-guide/join-streams-and-tables.html#joins-and-windows), leading to missing and potentially inaccurate data.

There are several ways you might have late arriving data, for example :

1) Unreliable or slow transmission of data
2) Manually entered data, including corrections to previously incorrectly entered data
3) Reingestion of archive data - e.g. a section of historical data was either missing or innaccurate in the initial ingestion and needs to be patched. 
4) Some stream calculation logic has been changed - though in this case, rather than reingesting the data at source, it is more appropriate to recalculate from the start of the original unmodified stream(s).  

## Simple inner joins

Assuming the stream `x` is our left stream and `y` is our right stream (see [README.md](./README.md). We can perform a KSQL join operation on any key field e.g. `key2`:

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

Similarly, you can use the `within` syntax, which is probably more efficient as it restricts the number of joined records by constraining the window :

```
ksql> select * from x inner join y within (5 seconds, 0 seconds) on x.key2 = y.key2;
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

If we leave the above query running, we can simulate a late-arriving record, e.g. a new y record at y.event_time = 7000 generates the following new records in the output stream:

```
7000 | k2 | 7000 | k1 | k2 | 7 | 7000 | k2 | 7000 | k1 | k2 | 70
8000 | k2 | 8000 | k1 | k2 | 8 | 7000 | k2 | 7000 | k1 | k2 | 70
9000 | k2 | 9000 | k1 | k2 | 9 | 7000 | k2 | 7000 | k1 | k2 | 70
10000 | k2 | 10000 | k1 | k2 | 10 | 7000 | k2 | 7000 | k1 | k2 | 70
11000 | k2 | 11000 | k1 | k2 | 11 | 7000 | k2 | 7000 | k1 | k2 | 70
12000 | k2 | 12000 | k1 | k2 | 12 | 7000 | k2 | 7000 | k1 | k2 | 70
```

This is good as it is generating the x records that have the new y record within their 'past' join window. If we can be sure that records arrive in order, this kind of query might be sufficient. The only downside is that a consumer would process _all_ joined records for each window, such that the last record within in the window 'wins' by overriding all previous ones;

Now, lets simulate out-of-order data, e.g. a new y record at y.event_time = 6500 :

```
7000 | k2 | 7000 | k1 | k2 | 7 | 6500 | k2 | 6500 | k1 | k2 | 65
8000 | k2 | 8000 | k1 | k2 | 8 | 6500 | k2 | 6500 | k1 | k2 | 65
9000 | k2 | 9000 | k1 | k2 | 9 | 6500 | k2 | 6500 | k1 | k2 | 65
10000 | k2 | 10000 | k1 | k2 | 10 | 6500 | k2 | 6500 | k1 | k2 | 65
11000 | k2 | 11000 | k1 | k2 | 11 | 6500 | k2 | 6500 | k1 | k2 | 65
```

So in this case, we again generate the x records that have the new record within their 'past' window. So for out-of-order data, a consumer would interpret the stream as meaning that these records are the 'latest' joins (in terms of event_time), and this would be incorrect. 


Note: re-running the query will correctly re-order the out-of-order records within each join group. However, a persistent query created with `create stream as` would maintain the order of the records, since records are written to an output topic in this case.

So, for two reasons (out-of-order records and sending multiple records for each join window), the basic join doesnt work well.

## Using group by

So we can consider `group by` which allows us to return a single record as well as recalculate it as new records arrive. We can group over the entire joined stream by x.event_time and any key (e.g. x.key2) :

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

This gives us _something like_ a join of each x record to the most recent y record. Its not quite right because we are using `max` and it only appears to be working because we have ingested data values that increase as event_time increases. We can write a custom [UDAF](https://www.confluent.io/blog/build-udf-udaf-ksql-5-0) to return the latest data within the group for each field. The implementation of such a function is [here](./java/udaf/latest/src/main/java/io/ninety/kafka/udaf/LatestUdaf.java), and it has been installed into the docker-compose environment.

However, the big problem with this approach is that the UDAFs aggregate records in order of processing time not event time. Furthermore, within the handlers of the UDAF you dont seem to be able to access the ROWTIME or any other fields in the record, so there is no way to determine whether the value being considered is more recent, in terms of event time, than the any of the others that have previously been processed in the group.

Alternatively, it you might think you can use the `collect_list` aggregate function, as below, but unfortunately the order of the array is ordered by processing time not the ROWTIME (i.e. event_time):

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

Since the implementation of a custom UDAF, would mirror very closely the functionality of `collect_list` then we would have the same ordering issues, and the UDAF handlers dont seem to have access to the ROWTIME, only the field value so it may be impossible to do this after all.


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

In the above results, the ROWTIME doesnt match the x.event_time (col3). For example, row 9 has a ROWTIME = 4000, but x.event_time = 5000.
We can fix this by specifying the TIMESTAMP field in the create table statement:

```
ksql>create table xy2  with(timestamp='event_time') as select x.event_time as event_time, x.key2 as key2, max(x.val) as x_val, max(y.val) as y_val from x left join y within 1 seconds on x.key2 = y.key2 where y.event_time is null or x.event_time >= y.event_time group by x.event_time, x.key2;
ksql> select * from xy2;
0 | 0|+|k2 | 0 | k2 | 0 | 10
1000 | 1000|+|k2 | 1000 | k2 | 1 | 10
2000 | 2000|+|k2 | 2000 | k2 | 2 | -2147483648
10000 | 10000|+|k2 | 10000 | k2 | 10 | -2147483648
12000 | 12000|+|k2 | 12000 | k2 | 12 | -2147483648
13000 | 13000|+|k2 | 13000 | k2 | 13 | -2147483648
4000 | 4000|+|k2 | 4000 | k2 | 4 | -2147483648
5000 | 5000|+|k2 | 5000 | k2 | 5 | 50
6000 | 6000|+|k2 | 6000 | k2 | 6 | 50
3000 | 3000|+|k2 | 3000 | k2 | 3 | -2147483648
4000 | 4000|+|k2 | 4000 | k2 | 4 | 30
4000 | 4000|+|k2 | 4000 | k2 | 4 | 40
5000 | 5000|+|k2 | 5000 | k2 | 5 | 50
5000 | 5000|+|k2 | 5000 | k2 | 5 | 50
9000 | 9000|+|k2 | 9000 | k2 | 9 | -2147483648
11000 | 11000|+|k2 | 11000 | k2 | 11 | -2147483648
3000 | 3000|+|k2 | 3000 | k2 | 3 | 30
4500 | 4500|+|k2 | 4500 | k2 | 4 | 40
4500 | 4500|+|k2 | 4500 | k2 | 4 | 45
7000 | 7000|+|k2 | 7000 | k2 | 7 | -2147483648
8000 | 8000|+|k2 | 8000 | k2 | 8 | -2147483648
```

Note: also that the KTable rows are assigned compound ROWKEYs of the `group by` fields :

```
ksql> select ROWTIME, ROWKEY from xy3;
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
My understanding is that this means that the underlying RocksDB will swap to disk and eventually fill the disk up. So, if we used this kind of join-and-aggregate approach, 
we would need to find a way to drop old records. Since KSQL is built on top of the Streams API, then you would have the same issue.

One way is to send [tombstone records](https://docs.confluent.io/current/ksql/docs/concepts/time-and-windows-in-ksql-queries.html#tumbling-window), which remove entries from the KTable. However, this
is not a good solution as it requires us to have another process sending these records.

Alternatively, if we were to drop the requirement to have an unwindowed aggregation, then the cache would be cleared-out based on the aggregation window retention time. So, given that we are trying to join to the latest y record
for each x record, such that x.event_time >= y.event_time, we need to consider which of the three aggregation [window types](https://docs.confluent.io/current/ksql/docs/developer-guide/syntax-reference.html#select)
(TUMBLING, HOPPING, SESSION), if any, are suitable for our needs :

- **TUMBLING** - There is a single window into which an x record falls (i.e. the [tumbling window's](https://docs.confluent.io/current/ksql/docs/concepts/time-and-windows-in-ksql-queries.html#tumbling-window) start time is inclusive
but the end time is exclusive). Furthermore, all the joined y records will occur in the same window, since we are grouping on x.event_time. There _can_, however, be multiple distinct 
x records within the same tumbling window (e.g. x records with 1min granularity, and window size of 5min),
but this is ok, because we are grouping on x.event time _within_ the window, and therefore we will get a single aggregate output record for each x record within the window. Which is what we want.
- **HOPPING** - Since x.event_time can fall within multiple windows (since they overlap), this will cause multiple reaggregation calculations on the same data, each leading to the same result, leading to duplicate aggregated output records.
- **SESSION** - can be discounted as we are assuming that we have a near continuous flow of data. 

Hence, our query becomes :

```
select x.event_time, x.key2, max(x.val), max(y.val)
from x
left join y within 1 seconds on x.key2 = y.key2
window tumbling (size 3 seconds)
where y.event_time is null or x.event_time >= y.event_time
group by x.event_time, x.key2;
```

A further issue is what to do about _very_ late data. We have two windows to consider :
- **JOIN WINDOW** - Extending the retention period for the join window will ensure we have more chance of having a non-null join with a y record. For data outside this retention period we would have to 
- **AGGREGATE WINDOW** - Extending the retention period for the aggregation window ensures that we capture any late arriving joined y records for any x record falling within the window.

Its also not clear how performant this is.


Finally, taking everything above into account we have the final create table statement :

```
ksql>create table x_to_latest_y with(timestamp='event_time') as
select
  x.event_time as event_time,
  x.key2 as key2,
  latest(x.val) as x_val,
  latest(y.val) as y_val
from x
left join y within 1 seconds on x.key2 = y.key2
window tumbling (size 3 seconds)
where y.event_time is null or x.event_time >= y.event_time
group by x.event_time, x.key2;
```

Note: We know that `latest()` does not work due to the ordering issue.
Note: The ROWKEY now gives us information about the window in which the aggregate was computed.


## Experiment with Stream Processors

What is clear is that the naieve approach of maintaining the latest y record in memory and joining it to each x record as it arrives, clearly does not work for many reasons. At a minimum, you have to cater for late-arriving records, so you _must_ cache recent stream data for x and y, and perform lookups and recalculations when late records arrive. This hand-crafted logic would be very similar to what is being done in the windowed join operation, but we can optimise a little since we are doing a very specific kind of join :

When a late x record, X,  arrives :

1) search the y cache for the nearest record Y, such that y.event_time < x.event_time.
2) join X to Y and emit record to stream

When a late y record, Y, arrives :

1) search the y cache for the next y record such that y.event_time > Y.event_time. Call this YN.
2) search the x cache for all the x records where x.event_time >= Y.event_time and x.event_time < YN.event_time
3) for all the found x records, join each one to the Y record and emit a stream record for each

Also we need to be able to integrate with the kafka streams state store, probably using [WindowStore](https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/state/WindowStore.html), which has a convenient [fetch](https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/state/ReadOnlyWindowStore.html#fetch-K-K-long-long-) function.

Notes:
- [JoinWindows](https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/JoinWindows.html) allows us to specify join windows that are in the past 
- https://stackoverflow.com/questions/50401792/how-to-write-the-valuejoiner-when-joining-two-kafka-streams-defined-using-avro-s

TBD
