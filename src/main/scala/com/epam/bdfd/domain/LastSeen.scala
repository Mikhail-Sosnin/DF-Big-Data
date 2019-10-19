package com.epam.bdfd.domain

/** Класс, представляющий информацию о последней активности пользователя
  *
  * @param platform игнорируется
  * @param time     время захода пользователя последний раз в сеть в секундах
  */
case class LastSeen(platform: String, time: String)
