package com.epam.bdfd.domain

/** Класс, представляющий информацию о подписках пользователя
  *
  * @param uid   id пользователя
  * @param items последовательность id подписок
  */
case class UserSubscriptions(uid: String, items: Seq[String])
