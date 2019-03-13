#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"

$dc exec -T broker kafka-topics --zookeeper "$zookeeper_addr" --delete --topic "$1"
