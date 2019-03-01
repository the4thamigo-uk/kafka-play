#!/bin/bash
source ./env.sh

docker-compose exec kafkacat kafkacat -b "$broker_addr" "${@:1}"

