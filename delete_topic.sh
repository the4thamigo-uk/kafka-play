#!/bin/bash
source ./env.sh

topic="$1"

docker-compose exec -T broker kafka-topics --zookeeper "$zookeeper_addr" --delete --topic "$1"
