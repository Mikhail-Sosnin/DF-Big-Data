#!/bin/bash
# copy runjob.sh
scp src/main/resources/runjob.sh uncommon-gull@18.194.143.9:/home/uncommon-gull/big_data_spark_workspace

# copy jar file
scp target/etl-1.0-SNAPSHOT.jar uncommon-gull@18.194.143.9:/home/uncommon-gull/big_data_spark_workspace
