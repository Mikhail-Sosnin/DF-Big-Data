package com.epam.bdfd.job

import java.sql.Timestamp
import java.time.LocalDateTime

import com.epam.bdfd.domain.{UserInfo, UserSubscriptions}
import com.epam.bdfd.util.Spark
import org.apache.spark.sql.Dataset
import com.epam.bdfd.util.StorageOperations._
import com.epam.bdfd.job.BdfdJob._
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/** Класс с описанием преобразований ETL-процесса
  *
  */
class BdfdJob extends Spark {
  def run(sourceFilePath: String,
          sourceMongoUri: String,
          archiveUserSubscriptionsPath: String,
          archiveUserInfoPath: String,
          starspaceOutPath: String): Unit = {
    import com.epam.bdfd.util.StorageOperations.DatasetWriter
    import ss.implicits._

    val userSubscriptionsDs: Dataset[UserSubscriptions] = readText(sourceFilePath)
      .flatMap(toUserSubscriptions)
    userSubscriptionsDs.writeParquet(archiveUserSubscriptionsPath)

    val userInfoDs: Dataset[UserInfo] = readMongo[UserInfo](sourceMongoUri)
      .persist(StorageLevel.DISK_ONLY)
    userInfoDs.writeParquet(archiveUserInfoPath)

    val starspaceOutput: Dataset[String] = userSubscriptionsDs
      .map(toStarspaceOut)
      .coalesce(1)
    starspaceOutput.writeText(starspaceOutPath)
  }
}

/** Объект-компаньон со вспомогательными методами
  *
  */
object BdfdJob {
  /** Ключи для join
    *
    */
  private val JOIN_KEY = "uid"
  private val FLATTEN_SUBSCRIPTIONS_KEY = "_2"
  private val SUBSCRIPTIONS_TO_FILTER_KEY = "value"

  /** Метод для преобразования строки в UserSubscriptions
    *
    * @param line входная строка в формате: userId<space>pubId1<space>pubId2
    * @return None, если входная строка не соответствует входному формату, Some(UserSubscriptions) в противном случае
    */
  private def toUserSubscriptions(line: String): TraversableOnce[UserSubscriptions] =
    line.split(" ").toList match {
      case id :: subscriptions => Some(UserSubscriptions(id, subscriptions))
      case _ => None
    }

  /** Метод для преобразования UserSubscriptions в строку для рекомендательной системы
    *
    * @param line входная запись UserSubscriptions
    * @return строка для рекомендательной системы в формате: public_pubId1<tab>public_pubId2
    */
  private def toStarspaceOut(line: UserSubscriptions): String =
    line
      .items
      .map("public_" + _)
      .mkString("\t")

  /** Метод для дедубликации
    *
    * @tparam T тип входной записи
    * @return Some(line), если запись уникальна, None в противном случае
    */
  private def dropDuplicates[T]: (String, Iterator[T]) => TraversableOnce[T] = {
    case (_, values) => values.toList match {
      case value :: Nil => Some(value)
      case _ => None
    }
  }

  /** Метод для проверки, заходил ли пользователь в социальную сеть недавно
    *
    * @param line входная запись UserInfo
    * @return true, если last_seen соответствует заданию, false в противном случае
    */
  private def notExpired(line: UserInfo): Boolean =
    Try(line.last_seen)
    .map(_.time)
    .map(_.toLong)
    .map(_ * 1000)
    .map(new Timestamp(_))
    .toOption
    .exists(_.after(Timestamp.valueOf(LocalDateTime.now().minusYears(3))))
}
