#!/bin/bash
set -eux

# navigate to the correct area for running Luigi pipeline
script_dir=`dirname $0`
cd $script_dir
cd src

# set env vars to defaults if not already set
N_RECORD=${N_RECORD:-50}
SLEEP_SECS=${SLEEP_SECS:-30}

# loop away
while true
do
  python -m luigi --module clickhouse_randomuser.luigi_pipeline  SchemaedCsvRows --workdir luigi-output --local-scheduler --DownloadRandomUsers-n-record ${N_RECORD}
  sleep ${SLEEP_SECS}
done
