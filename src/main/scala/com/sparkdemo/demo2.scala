package com.sparkdemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object demo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder ()
    .master ("local[*]")
    .appName ("SparkDemo04")
    .getOrCreate ()

    val sfOptions = Map (
    "sfURL" -> "https://aub55594.us-east-1.snowflakecomputing.com",
    "sfUser" -> "lcorral",
    "sfPassword" -> "Password1",
    "sfDatabase" -> "test_db",
    "sfSchema" -> "test_schema",
    "sfWarehouse" -> "test_dw",
    "sfRole" -> "accountadmin"
    )

    val df: DataFrame = spark.read
    .format ("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options (sfOptions)
    .option("dbtable", "test_table")
    //.option("query", "select id, name, preferences, created_at from big_data_table1")
    .load ()

    df.show (false)
  }
}