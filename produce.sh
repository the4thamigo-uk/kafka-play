#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"
key_schema="$(cat $script_dir/schema/key.avsc)"
value_schema="$(cat $script_dir/schema/value.avsc)"

$dc exec -T connect kafka-avro-console-producer --broker-list "$broker_addr" --topic "$topic" --property schema.registry.url="$schema_registry_url" --property parse.key=true --property key.schema="$key_schema" --property value.schema="$value_schema"
