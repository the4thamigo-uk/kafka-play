#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"

$script_dir/kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic "$topic"
