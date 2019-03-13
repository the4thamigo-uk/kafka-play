#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"

$dc exec broker kafka-topics --create --zookeeper "$zookeeper_addr" --replication-factor 1 --partitions 1 --topic "$topic"
