#!/bin/bash
source ./env.sh

topic="$1"

docker-compose exec broker kafka-topics --create --zookeeper "$zookeeper_addr" --replication-factor 1 --partitions 1 --topic "$topic"
