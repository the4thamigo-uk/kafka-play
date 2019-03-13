# Joiner

_Kafka stream processor to join two AVRO streams such that each left record is joined only to the right record that is nearest to it, and before it, in time_

## Getting Started

Build the jar:

```
mvn package
```

Restart a clean docker compose environment, import dummy data, and get a bash prompt in the broker

```
../../../purge.sh
../../../x_y_data.sh
../../../broker.sh
```

Once the compose environment has fully started, launch the stream processor from the broker prompt:

```
root@broker:/# java -jar /stream_processors/joiner-1.0-SNAPSHOT-jar-with-dependencies.jar
```






