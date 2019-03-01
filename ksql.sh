#!/bin/bash
source ./env.sh

docker-compose exec ksql-cli ksql  --config-file /ksql_cli.config "$ksqlserver_url" "${@:1}"

