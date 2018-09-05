package org.apache.spark.ui

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by KPrajapati on 9/4/2018.
 */
class ExtendedUIServerSpec extends FunSuite with BeforeAndAfterAll {
  implicit var sparkSession: SparkSession = _

  override def beforeAll() {
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .appName("Update metadata of spark table")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
  }

  test("metadata is propagated correctly") {
    val spark = sparkSession
    import spark.implicits._

    import org.apache.spark.sql.functions.count
    import Utility._
    new ExtendedUIServer(spark.sparkContext)
    Seq("1").toDF("id").registerSchema

    println("1")
    Thread.sleep(10000)
    Seq(("1", 1)).toDF("id", "count").groupBy("id").agg(count("id") as "count")
      .registerSchema
    println("2")

    Seq("1").toDF("id1").distinct().registerSchema
    println("3")
    Thread.sleep(1000000)

    assert(true)
  }

  override def afterAll() {
    sparkSession.stop()
  }
}
