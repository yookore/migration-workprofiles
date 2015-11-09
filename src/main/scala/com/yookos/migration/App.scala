package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object App extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  if (mode == "yarn") {
    sc.addJar("hdfs:///user/hadoop-user/data/jars/postgresql-9.4-1200-jdbc41.jar")
    sc.addJar("hdfs:///user/hadoop-user/data/jars/migration-workprofiles-0.1-SNAPSHOT.jar")
  }
  createSchema(conf)

  implicit val formats = DefaultFormats
  
  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_workprofiles_index") == null) 0 else cache.get("latest_legacy_workprofiles_index").toInt

  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers"
    )
  )

  val legacyDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jiveuserprofile")
  )
  
  val profiles = sc.cassandraTable[Work](s"$keyspace", "legacyworkprofiles").cache()

  val df = mappingsDF.select(mappingsDF("userid"), mappingsDF("yookoreid"))

  reduce(df)

  private def reduce(mdf: DataFrame) = {
    mdf.collect().foreach(row => {
      val yookoreid = row.getString(1)
      profiles.filter(csp => csp.userid == yookoreid).collect().foreach {
        profile =>
          cachedIndex = cachedIndex + 1
          cache.set("latest_legacy_workprofiles_index", cachedIndex.toString)
          val userid = row.getLong(0)
          upsert(row, profile, userid)
      }
    })
  }

  private def upsert(row: Row, profile: Work, jiveuserid: Long) = {
    legacyDF.select(legacyDF("fieldid"), legacyDF("value"), legacyDF("userid")).filter(f"userid = $jiveuserid%d").collect().map {
        profileRow =>
          val fieldid = profileRow.getInt(0)
          val value = profileRow.getString(1)
          val userid = row.getString(1)
          val username = profile.username
          val creationdate = profile.creationdate
          val lastupdated = profile.lastupdated
         
          fieldid match {
            case 2 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val department = Some(value)
              save(Seq(Work(department, creationdate,
                lastupdated, Some(null), username,
                userid, Some(null), Some(null),
                Some(null), Some(null))))

            case 7 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val startdate = Some(value)
              save(Seq(Work(Some(null), creationdate,
                lastupdated, startdate, username,
                userid, Some(null), Some(null),
                Some(null), Some(null))))

            // expertise
            case 9 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val expertise = Some(value)
              save(Seq(Work(Some(null), creationdate,
                lastupdated, Some(null), username,
                userid, expertise, Some(null), Some(null),
                Some(null))))

            case 5015 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val company = Some(value)
              save(Seq(Work(Some(null), creationdate,
                lastupdated, Some(null), username,
                userid, Some(null), company, 
                Some(null), Some(null))))

            case 5019 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val jobtitle = Some(value)
              save(Seq(Work(Some(null), creationdate,
                lastupdated, Some(null), username,
                userid, Some(null), Some(null),
                jobtitle, Some(null))))

            case 11 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val address = Some(value)
              save(Seq(Work(Some(null), creationdate,
                lastupdated, Some(null), username,
                userid, Some(null), Some(null),
                Some(null), address)))
          }
          
          println("===Latest workprofiles cachedIndex=== " + cache.get("latest_legacy_workprofiles_index").toInt)
      }
  }

  private def save(workprofile: Seq[Work]) = workprofile match {
    case Nil => println("Nil")
    case List(w @ _*) => 
      sc.parallelize(workprofile)
        .saveToCassandra(s"$keyspace", "legacyworkprofiles", 
          SomeColumns("department", "creationdate",
            "lastupdated", "hiredate", "username", "userid",
            "expertise", "company", "jobtitle", "address")
        )
  }

  def p(field: Int, value: String): Map[String, String] = field match {
    case 1 => Map("title" -> value)
    case 5006 => Map("title" -> value)
    case 8 => Map("biography" -> value)
    case 5001 => Map("gender" -> value)
    case 5009 => Map("country" -> value)
    case 5012 => Map("relationshipstatus" -> value)
    case 5002 => Map("birthdate" -> value)
    case 3 => Map("address" -> value)
    case 4 => Map("phonenumber" -> value)
    case 5 => Map("homephonenumber" -> value)
    case 6 => Map("mobile" -> value)
    case 7 => Map("hiredate" -> value)
    case 9 => Map("expertise" -> value)
    case 10 => Map("alternateemail" -> value)
    case 11 => Map("homeaddress" -> value)
    case 12 => Map("location" -> value)
    case 2 => Map("department" -> value)
    case 5015 => Map("company" -> value)
    case 5018 => Map("counter" -> value)
    case 5010 => Map("hobbies_interest" -> value)
    case 5019 => Map("jobtitle" -> value)
    case 5020 => Map("education" -> value)
  }

  mappingsDF.printSchema()
  
  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyworkprofiles (department text, hiredate text, userid text, username text, expertise text, company text, jobtitle text, address text, creationdate text, lastupdated text, PRIMARY KEY (userid, username)) WITH CLUSTERING ORDER BY (username DESC)")
    } wasApplied
  }
}
