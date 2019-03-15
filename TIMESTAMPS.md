# Timestamps

## Introduction

In kafka a record has a key and a timestamp. The timestamp is used for considering when data records should be deleted, as well as in windowing (e.g. joins/aggregations).

Create some data on topics x and y:

```
$ cat ./data/x_deletion.dat | ./produce.sh x
```

Timestamps are automatically created by kafka at the moment of ingestion:

```
$ ./kafkacat.sh -C -t x -f "%T\n"
1552652474765
1552652474800
1552652474801
1552652474802
1552652474802
1552652474806
1552652474807
``````

However, if we create a streams using event_time for x and y, our ROWTIME reflects the the event_time :

```
ksql> create stream x with (kafka_topic='x', value_format='avro', timestamp='event_time');
ksql> select ROWTIME from x;
0
1000
2000
3000
4000
1552680010000
10000
```

We can persist the output of this ksql query to another stream :

```
ksql> create stream x2 as select * from x;
ksql> select ROWTIME from x2;
0
1000
2000
3000
4000
1552680010000
10000
```
