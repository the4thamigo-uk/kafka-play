#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

$dc exec ksql-cli ksql  --config-file /ksql_cli.config "$ksqlserver_url" "${@:1}"

