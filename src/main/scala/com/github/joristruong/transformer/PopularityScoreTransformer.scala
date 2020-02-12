package com.github.joristruong.transformer

import com.github.joristruong.entity.{VideoScore, VideoStats}
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class PopularityScoreTransformer(videosStats: Dataset[VideoStats],
                                 viewsWeight: Double,
                                 trendingDaysWeight: Double,
                                 likesRatioWeight: Double,
                                 commentsWeight: Double
                                ) extends Transformer[Dataset[VideoScore]] with HasSparkSession {

  import spark.implicits._

  var transformedData: Dataset[VideoScore] = _

  override def transformed: Dataset[VideoScore] = transformedData

  override def transform(): PopularityScoreTransformer.this.type = {
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
        "country",
        "score"
      )
      .as[VideoScore]

    this
  }
}
