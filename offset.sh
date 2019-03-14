#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

topic="$1"

$dc exec broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$topic"
