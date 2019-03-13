#!/bin/bash
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $script_dir/env.sh

cat $script_dir/data/x.dat | $script_dir/produce.sh x
cat $script_dir/data/y.dat | $script_dir/produce.sh y
