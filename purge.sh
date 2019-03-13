#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

$script_dir/down.sh
sudo rm -rf $script_dir/kafka_data
$script_dir/up.sh
