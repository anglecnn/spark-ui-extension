package com.github.skp33

import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.{ExtendedUIServer, Utility}

/**
 * Created by KPrajapati on 9/5/2018.
 */
object TestUIExtension {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .appName("Update metadata of spark table")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    new ExtendedUIServer(spark.sparkContext)

    import spark.implicits._
    import Utility._
    Seq("1").toDF("id").registerSchema
    println("First Dataframe")
    Thread.sleep(10000)

    import org.apache.spark.sql.functions.count
    Seq(("1", 1)).toDF("id", "count").groupBy("id").agg(count("id") as "count")
      .registerSchema
    println("Second Dataframe")

    Seq("1").toDF("otherId").distinct().registerSchema.show
    println("Third Dataframe")
    Thread.sleep(60000)
    println("Test Done ..")
  }
}
