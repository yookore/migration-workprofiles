package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import org.apache.commons.lang.StringEscapeUtils

case class Work(department: Option[String], 
                  creationdate: String, 
                  lastupdated: String,
                  hiredate: Option[String],
                  username: String,
                  userid: String,
                  expertise: Option[String],
                  company: Option[String],
                  jobtitle: Option[String],
                  address: Option[String]
                  )extends Serializable

object Work {
  implicit object Mapper extends DefaultColumnMapper[Work](
    Map("department" -> "department", 
      "creationdate" -> "creationdate",
      "lastupdated" -> "lastupdated",
      "hiredate" -> "hiredate",
      "username" -> "username",
      "expertise" -> "expertise",
      "company" -> "company",
      "jobtitle" -> "jobtitle",
      "address" -> "address",
      "userid" -> "userid"))
}
