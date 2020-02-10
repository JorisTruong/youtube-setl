package com.github.joristruong.factory

import com.github.joristruong.entity.{VideoScore, VideoStats}
import com.github.joristruong.transformer.ComputePopularityScoreTransformer
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class PopularityScoreFactory extends Factory[Dataset[VideoScore]] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "viewsWeight") private[this] val viewsWeight: Double = 0D
  @Delivery(id = "trendingDaysWeight") private[this] val trendingDaysWeight: Double = 0D
  @Delivery(id = "likesRatioWeight") private[this] val likesWeight: Double = 0D
  @Delivery(id = "commentsWeight") private[this] val commentsWeight: Double = 0D

  @Delivery(autoLoad = true)
  private[this] val videosStats = spark.emptyDataset[VideoStats]

  private[this] var output: Dataset[VideoScore] = _

  override def read(): PopularityScoreFactory.this.type = this

  override def process(): PopularityScoreFactory.this.type = {
    output = new ComputePopularityScoreTransformer(
      videosStats,
      viewsWeight,
      trendingDaysWeight,
      likesWeight,
      commentsWeight
    ).transform().transformed

    this
  }

  override def write(): PopularityScoreFactory.this.type = {
    output.show()
    this
  }

  override def get(): Dataset[VideoScore] = output
}
