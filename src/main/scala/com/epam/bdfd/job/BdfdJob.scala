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
  /** Метод для запуска ETL-преобразования
    *
    * @param sourceFilePath               путь к директории users_subscriptions_data_2019 в S3
    * @param sourceMongoUri               адрес коллекции vk_db.user_info в MongoDB
    * @param archiveUserSubscriptionsPath путь к архивной директории user_subscriptions_archive в S3
    * @param archiveUserInfoPath          путь к архивной директории user_info_archive в S3
    * @param starspaceOutPath             путь к директории с выходным файлом starspace_output в S3 для обучения рекомендательной системы
    */
  def run(sourceFilePath: String,
          sourceMongoUri: String,
          archiveUserSubscriptionsPath: String,
          archiveUserInfoPath: String,
          starspaceOutPath: String): Unit = {
    import com.epam.bdfd.util.StorageOperations.DatasetWriter
    import ss.implicits._

    // подписки пользователей
    val userSubscriptionsDs: Dataset[UserSubscriptions] = readText(sourceFilePath)
      .flatMap(toUserSubscriptions)
    //TODO: дедубликация (groupByKey, flatMapGroups)

    userSubscriptionsDs.writeParquet(archiveUserSubscriptionsPath)

    // информация о пользователях
    val userInfoDs: Dataset[UserInfo] = readMongo[UserInfo](sourceMongoUri)
      .persist(StorageLevel.DISK_ONLY)
    // TODO: дедубликация (groupByKey, flatMapGroups)

    userInfoDs.writeParquet(archiveUserInfoPath)

    // объединенные подписки пользователей с информацией о пользователях
    val joined: Dataset[(UserSubscriptions, UserInfo)] =
    // TODO: join двух Dataset (joinWith)

    // активные пользователи социальной сети
    val activeUsersDs: Dataset[UserSubscriptions] =
    // TODO: фильтрация joined по last_seen и deactivated (filter)
    // TODO: преобразование в UserSubscriptions (map)

    // редко встречающиеся подписки для фильтрации
    val subscriptionsToFilter: Dataset[String] = activeUsersDs
      .flatMap(_.items)
    // TODO: подсчет количества каждой подписки в activeUsersDs (groupByKey, count)
    // TODO: фильтрация редко встречающихся подписок (filter)
    // TODO: преобразование в id подписки (map)

    // `плоские` пользователи с подписками
    val flattenSubscriptions: Dataset[(String, String)] = activeUsersDs
      .flatMap { subscriptions => subscriptions.items.map((subscriptions.uid, _)) }

    // результирующее представление для обучения рекомендатеьной системы
    val starspaceOutput: Dataset[String] =
    // TODO: join flattenSubscriptions с subscriptionsToFilter для дальнейшей фильтрации (joinWith); ключи для join в объекте-компаньоне
      .filter(_ match {
        case (_, toFilter) => toFilter == null
      })
      .map { case (subscriptions, _) => subscriptions }
      .groupByKey { case (uid, _) => uid }
      .mapGroups {
        case (uid, subscriptions) => (uid, subscriptions.toList.map { case (_, subscriptionId) => subscriptionId })
      }
      .withColumnRenamed("_1", "uid")
      .withColumnRenamed("_2", "items")
      .as[UserSubscriptions]
    // TODO: фильтрация пользователей с небольшим количеством подписок (filter)
    // TODO: приведение к выходному виду для обучения рекомендатеьной системы (map)
    // TODO: подготовка к записи одного файла (coalesce)

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
