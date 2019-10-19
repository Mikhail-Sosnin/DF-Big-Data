#!/usr/bin/env bash
export SPARK_MAJOR_VERSION=2
export HADOOP_USER_NAME=hdfs
spark-submit                                    \
    --name bdfd-etl                             \
    --class com.epam.bdfd.Runner                \
    --master yarn                                \
    --deploy-mode cluster                           \
    --executor-memory 4G                       \
    --executor-cores 2                        \
    --num-executors 6                         \
    --driver-memory 4G                         \
    etl-1.0-SNAPSHOT.jar                        \
    -t s3a://nonauth-common-friendsday-noqzfl/users_subscriptions_data_2019_sample  \
    -m mongodb://ec2-3-120-132-157.eu-central-1.compute.amazonaws.com/vk_db.user_info            \
    -s s3a://XXX/user_subscriptions_archive     \
    -i s3a://XXX/user_info_archive              \
    -d s3a://XXX/starspace_output
