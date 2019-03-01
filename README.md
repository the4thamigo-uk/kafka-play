# kafka play

Basic environment for playing with kafka/confluent, particularly ksql and java stream processors

Getting started

Ensure you have latest docker-compose (https://docs.docker.com/compose/install/).
Note: Current version in Ubuntu repository (1.17.1) has a known issuue where is fails to pipe data into `docker exec -T command`.

```
docker-compose up
```

You will have to wait awhile for all the services to start and connect with each other.

Check everything is running [here](http://localhost:9021) 


Import two basic datasets (which share the same avro schema), into two topics 'x' and 'y' :

```
cat ./data/x.dat | ./produce.sh x
cat ./data/y.dat | ./produce.sh y
```

Check topic is created [here](http://localhost:9021/management/topics)

Start the ksql cli :

```
./ksql.sh
```

Create a stream for each topic, using the event_time from the avro record :

```
create stream x with (kafka_topic='x', value_format='avro', timestamp='event_time');
create stream y with (kafka_topic='y', value_format='avro', timestamp='event_time');
```

Check they are returning data:

```
ksql> select * from x;
0 | rk1 | 0 | k1 | k2 | 0
1000 | rk1 | 1000 | k1 | k2 | 1
2000 | rk1 | 2000 | k1 | k2 | 2
3000 | rk1 | 3000 | k1 | k2 | 3
4000 | rk1 | 4000 | k1 | k2 | 4
5000 | rk1 | 5000 | k1 | k2 | 5
6000 | rk1 | 6000 | k1 | k2 | 6
7000 | rk1 | 7000 | k1 | k2 | 7
8000 | rk1 | 8000 | k1 | k2 | 8
9000 | rk1 | 9000 | k1 | k2 | 9
10000 | rk1 | 10000 | k1 | k2 | 10
11000 | rk1 | 11000 | k1 | k2 | 11
12000 | rk1 | 12000 | k1 | k2 | 12
13000 | rk1 | 13000 | k1 | k2 | 13

ksql> select * from y;
0 | rk1 | 0 | k1 | k2 | 10
5000 | rk1 | 5000 | k1 | k2 | 50
0 | rk1 | 0 | k1 | k2 | 10
5000 | rk1 | 5000 | k1 | k2 | 50

```

Note: we have set the ksql [configuration](./ksql_cli.config) to always return data from the start of the stream)


# Further Experimentation

[Join stream to latest record in another stream](./JOIN_TO_LATEST.md)
