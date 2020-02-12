package com.github.joristruong

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WithoutSetl {

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
      .distinct()
  }

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    ///////////////////
    /// READ VIDEOS ///
    ///////////////////

    val CAVideos = readVideo("src/main/resources/CAvideos.csv", "CA")
    val DEVideos = readVideo("src/main/resources/DEvideos.csv", "DE")
    val FRVideos = readVideo("src/main/resources/FRvideos.csv", "FR")
    val GBVideos = readVideo("src/main/resources/GBvideos.csv", "GB")
    val INVideos = readVideo("src/main/resources/INvideos.csv", "IN")
    val JPVideos = readVideo("src/main/resources/JPvideos.csv", "JP")
    val KRVideos = readVideo("src/main/resources/KRvideos.csv", "KR")
    val MXVideos = readVideo("src/main/resources/MXvideos.csv", "MX")
    val RUVideos = readVideo("src/main/resources/RUvideos.csv", "RU")
    val USVideos = readVideo("src/main/resources/USvideos.csv", "US")

    val videos = Seq(
      CAVideos,
      DEVideos,
      FRVideos,
      GBVideos,
      INVideos,
      JPVideos,
      KRVideos,
      MXVideos,
      RUVideos,
      USVideos
    )

    val allVideos = videos.reduce(_.union(_))


    //////////////////////
    /// PROCESS VIDEOS ///
    //////////////////////

    val viewsWeight = 0.6
    val trendingDaysWeight = 0.25
    val likesRatioWeight = 0.1
    val commentsWeight = 0.05


    // Get the latest stats
    val w = Window
      .partitionBy(
        "video_id",
        "title",
        "channel_title",
        "category_id",
        "comments_disabled",
        "country"
      )

    val w2 = w.orderBy($"trending_date".desc)

    val latestStatsVideos = allVideos
      .withColumn("trending_days", count($"video_id").over(w).cast("int"))
      .withColumn("rank", rank().over(w2))
      .filter(row => row.getAs[Int]("rank") == 1)
      .drop("rank")
      .sort($"country", $"trending_days".desc, $"views".desc, $"likes".desc)

    // Compute popularity score
    val normalizedLikeDislike = latestStatsVideos
      .withColumn("normalizedLikes", $"likes" / $"views")
      .withColumn("normalizedDislikes", $"dislikes" / $"views")
      .withColumn("normalizedLikePercentage", $"normalizedLikes" / ($"normalizedLikes" + $"normalizedDislikes"))
      .drop("normalizedLikes", "normalizedDislikes")

    val normalizedCommentCount = normalizedLikeDislike
      .withColumn("normalizedCommentsCount", $"comment_count" / $"views")

    val score = normalizedCommentCount
      .withColumn("score",
        when($"comments_disabled",
          $"views" * lit(viewsWeight) +
            $"trending_days" * lit(trendingDaysWeight) +
            $"normalizedLikePercentage" * lit(likesRatioWeight + commentsWeight)
        )
          .otherwise(
            $"views" * lit(viewsWeight) +
              $"trending_days" * lit(trendingDaysWeight) +
              $"normalizedLikePercentage" * lit(likesRatioWeight) +
              $"normalizedCommentsCount" * lit(commentsWeight)
          )
      )
      .sort($"score".desc)
      .select(
        "video_id",
        "title",
        "channel_title",
        "category_id",
        "views",
        "likes",
        "dislikes",
        "comment_count",
        "comments_disabled",
        "country",
        "trending_days",
        "score"
      )

    score.show(100, false)
  }

}
