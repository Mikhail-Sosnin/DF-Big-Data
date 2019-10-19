package com.epam.bdfd.util

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.SaveMode.Overwrite

import scala.reflect.runtime.universe.TypeTag

/** Операции чтения/записи на Dataset
  *
  */
object StorageOperations extends Spark {

  /** Класс для методов на Dataset[T]
    *
    * @param ds Dataset, на котором вызывабтся методы класса
    * @tparam T тип Dataset
    */
  implicit class DatasetWriter[T: Encoder](ds: Dataset[T]) {
    def writeText(path: String): Unit = ds.write.mode(Overwrite).text(path)

    def writeParquet(path: String): Unit = ds.write.mode(Overwrite).parquet(path)
  }

  def readMongo[T <: Product : Encoder : TypeTag](uri: String): Dataset[T] =
    MongoSpark.load[T](ss, ReadConfig(Map("uri" -> uri))).as[T]

  def readText(path: String): Dataset[String] = ss.read.textFile(path)
}
