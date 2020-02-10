package com.github.joristruong

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object utils {

  val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("youtube-etl")
    .getOrCreate

  def readVideo(path: String, region: String): DataFrame = {
    val originalDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("dateFormat", "yy.MM.dd")
      .load(path)

    originalDF
      .select(
        "video_id",
        "title",
        "channel_title",
        "category_id",
        "trending_date",
        "views",
        "likes",
        "dislikes",
        "comment_count",
        "comments_disabled",
        "video_error_or_removed"
      )
      .withColumn("country", lit(region))
      .filter(row => !row.getAs[Boolean]("video_error_or_removed"))
  }
}
