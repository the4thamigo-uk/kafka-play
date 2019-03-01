#!/bin/bash
source ./env.sh

topic="$1"

docker-compose exec -T connect kafka-avro-console-consumer --from-beginning  --bootstrap-server "$broker_addr" --topic "$topic" --property print.key=true --property schema.registry.url="$schema_registry_url"

