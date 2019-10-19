package com.epam.bdfd

import com.epam.bdfd.job.BdfdJob
import org.rogach.scallop.{ScallopConf, ScallopOption}

/** Парсер аргументов запуска программы
  *
  * @param args аргументы
  */
class RunnerArgs(args: Seq[String]) extends ScallopConf(args) {
  val sourceTxtPath: ScallopOption[String] = opt[String](
    short = 't',
    descr = "user_subscriptions txt path",
    required = true
  )

  val sourceMongoUri: ScallopOption[String] = opt[String](
    short = 'm',
    descr = "user_info mongo uri",
    required = true
  )

  val userSubscriptionsArchivePath: ScallopOption[String] = opt[String](
    short = 's',
    descr = "user_subscriptions archive path",
    required = true
  )

  val userInfoArchivePath: ScallopOption[String] = opt[String](
    short = 'i',
    descr = "user_info archive path",
    required = true
  )

  val staspaceOutputPath: ScallopOption[String] = opt[String](
    short = 'd',
    descr = "starspace output path",
    required = true
  )

  verify()
}

/** Точка входа в программу
  *
  */
object Runner extends App {
  val runnerArgs = new RunnerArgs(args)

  new BdfdJob().run(
    runnerArgs.sourceTxtPath(),
    runnerArgs.sourceMongoUri(),
    runnerArgs.userSubscriptionsArchivePath(),
    runnerArgs.userInfoArchivePath(),
    runnerArgs.staspaceOutputPath()
  )
}
