#!/usr/bin/env bash
export SPARK_MAJOR_VERSION=2
export HADOOP_USER_NAME=hdfs
# TODO: заполнить ??? необходимой конфигурацией и XXX uri MongoDB и путями к S3 buckets
spark-submit                                    \
    --name bdfd-etl                             \
    --class com.epam.bdfd.Runner                \
    --master ???                                \
    --deploy-mode ???                           \
    --executor-memory ???                       \
    --executor-cores ???                        \
    --num-executors ???                         \
    --driver-memory ???                         \
    etl-1.0-SNAPSHOT.jar                        \
    -t s3a://XXX/users_subscriptions_data_2019  \
    -m mongodb://XXX/vk_db.user_info            \
    -s s3a://XXX/user_subscriptions_archive     \
    -i s3a://XXX/user_info_archive              \
    -d s3a://XXX/starspace_output
