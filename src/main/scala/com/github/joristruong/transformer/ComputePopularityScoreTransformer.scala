package com.github.joristruong.transformer

import com.github.joristruong.entity.VideoStats
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class ComputePopularityScoreTransformer(
                                         videosStats: Dataset[VideoStats],
                                         viewsWeight: Double,
                                         trendingDaysWeight: Double,
                                         likesRatioWeight: Double,
                                         commentsWeight: Double
                                       ) extends Transformer[Dataset[VideoStats]] with HasSparkSession {

  import spark.implicits._

  var transformedData: Dataset[VideoStats] = _

  override def transformed: Dataset[VideoStats] = transformedData

  override def transform(): ComputePopularityScoreTransformer.this.type = {
    val normalizedLikeDislike = videosStats
      .withColumn("normalizedLikes", $"likes" / $"views")
      .withColumn("normalizedDislikes", $"dislikes" / $"views")
      .withColumn("normalizedLikePercentage", $"normalizedLikes" / ($"normalizedLikes" + $"normalizedDislikes"))
      .drop("normalizedLikes", "normalizedDislikes")

    val normalizedCommentCount = normalizedLikeDislike
      .withColumn("normalizedCommentsCount", $"commentCount" / $"views")

    val score = normalizedCommentCount
      .withColumn("score",
        when($"commentDisabled",
          $"views" * lit(viewsWeight) +
            $"trendingDays" * lit(trendingDaysWeight) +
            $"normalizedLikePercentage" * lit(likesRatioWeight + commentsWeight)
        )
          .otherwise(
            $"views" * lit(viewsWeight) +
              $"trendingDays" * lit(trendingDaysWeight) +
              $"normalizedLikePercentage" * lit(likesRatioWeight) +
              $"normalizedCommentsCount" * lit(commentsWeight)
          )
      )
      .sort($"score".desc)

    transformedData = score
      .select(
        "videoId",
        "title",
        "channelTitle",
        "categoryId",
        "trendingDate",
        "views",
        "likes",
        "dislikes",
        "commentCount",
        "commentDisabled",
        "country",
        "trendingDays"
      )
      .as[VideoStats]

    this
  }
}
