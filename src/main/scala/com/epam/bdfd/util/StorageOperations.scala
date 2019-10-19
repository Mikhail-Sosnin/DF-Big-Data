package com.epam.bdfd.util

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{Dataset, Encoder}

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
    /** Метод для записи Dataset текстовые файлы
      *
      * @param path путь до директории
      */
    def writeText(path: String): Unit = ??? // TODO: реализовать метод

    /** Метод для записи Dataset в parquet-файлы
      *
      * @param path путь до директории
      */
    def writeParquet(path: String): Unit = ??? // TODO: реализовать метод
  }

  /** Метод для чтения Dataset из MongoDB
    *
    * @param uri адрес коллекции для чтения
    * @tparam T тип Dataset
    * @return Dataset типа T со всеми записями из коллекции MongoDB
    */
  def readMongo[T <: Product : Encoder : TypeTag](uri: String): Dataset[T] =
    MongoSpark.load[T](ss, ReadConfig(Map("uri" -> uri))).as[T]

  /** Метод для чтения Dataset из текстового файла
    *
    * @param path путь до файла/директории
    * @return Dataset[String] со всеми записями из файла/директории
    */
  def readText(path: String): Dataset[String] = ??? // TODO: реализовать метод
}
