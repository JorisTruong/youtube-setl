package com.github.joristruong

import com.github.joristruong.utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import spark.implicits._

object AppETL {

  def main(args: Array[String]): Unit = {

    //////////////////////
    /////// VIDEOS ///////
    //////////////////////


    ///////////////////
    /// READ VIDEOS ///
    ///////////////////

    val CAVideos = readVideo("src/main/resources/inputs/videos/CAvideos.csv", "CA").cache()
    val DEVideos = readVideo("src/main/resources/inputs/videos/DEvideos.csv", "DE").cache()
    val FRVideos = readVideo("src/main/resources/inputs/videos/FRvideos.csv", "FR").cache()
    val GBVideos = readVideo("src/main/resources/inputs/videos/GBvideos.csv", "GB").cache()
    val INVideos = readVideo("src/main/resources/inputs/videos/INvideos.csv", "IN").cache()
    val JPVideos = readVideo("src/main/resources/inputs/videos/JPvideos.csv", "JP").cache()
    val KRVideos = readVideo("src/main/resources/inputs/videos/KRvideos.csv", "KR").cache()
    val MXVideos = readVideo("src/main/resources/inputs/videos/MXvideos.csv", "MX").cache()
    val RUVideos = readVideo("src/main/resources/inputs/videos/RUvideos.csv", "RU").cache()
    val USVideos = readVideo("src/main/resources/inputs/videos/USvideos.csv", "US").cache()

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

    score.show(1000, false)
  }

}
