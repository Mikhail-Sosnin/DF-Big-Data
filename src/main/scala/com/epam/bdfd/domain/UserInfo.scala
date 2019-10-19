package com.epam.bdfd.domain

/** Класс, представляющий информацию о пользователе
  *
  * @param uid         id пользователя
  * @param last_seen   последняя активность  пользователя
  * @param deactivated информация о деактивации (null - пользователь активен)
  */
case class UserInfo(uid: String, last_seen: LastSeen, deactivated: String)
