package com.epam.bdfd.util

import org.apache.spark.sql.SparkSession

/** Trait, предоставляющий сессию Spark в месте добавления
  *
  */
trait Spark {
  lazy val ss: SparkSession = SparkSession
    .builder()
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .getOrCreate()
}
