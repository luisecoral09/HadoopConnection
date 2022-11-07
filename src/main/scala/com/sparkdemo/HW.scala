package com.sparkdemo

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object HW {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDemo04")
      .getOrCreate()
    val sfOptions = Map(
      "sfURL" -> "https://aub55594.us-east-1.snowflakecomputing.com",
      "sfUser" -> "lcorral",
      "sfPassword" -> "Password1",
      "sfDatabase" -> "votehw_db",
      "sfSchema" -> "votes_schema",
      "sfWarehouse" -> "test_dw",
      "sfRole" -> "accountadmin"
    )
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    val votesschema = StructType(Array(
      StructField("BountyAmount", StringType, true),
      StructField("CreationDate", DateType, true),
      StructField("Id", StringType, true),
      StructField("PostId", StringType, true),
      StructField("UserId", StringType, true),
      StructField("VoteTypeId", StringType, true)
    ))

    val df: DataFrame = spark.read.schema(votesschema).option("multiline","true").json("C:\\Users\\Consultant\\Downloads\\Votes.json")
    df.printSchema()
    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "votes")
      .mode(SaveMode.Overwrite)
      .save()

    val df2: DataFrame = spark.read
      .format(SNOWFLAKE_SOURCE_NAME) // or just use "snowflake"
      .options(sfOptions)
      .option("query", "SELECT \"# Of Records\", \"Year\", \"Week\" FROM (SELECT *, lag(\"Week\", 1, 0) over " +
        "(ORDER BY \"Year\", \"Week\") as \"Prev Week\" FROM (SELECT count(*) as \"# Of Records\", year(creationdate)" +
        "as \"Year\", week(creationdate) as \"Week\", sum(count(*)) OVER()/count(*) OVER() as average FROM votes " +
        "GROUP BY year(creationdate), week(creationdate))tb1 WHERE \"# Of Records\" > average * 1.2 or \"# Of Records\" " +
        "< average * 0.8)tb2 WHERE \"Week\" != CASE WHEN \"Prev Week\"+1 > 52 AND \"Week\" = 1 THEN 1 ELSE \"Prev Week\"+1 END")
        .load()
    df2.show(false)
  }
}
