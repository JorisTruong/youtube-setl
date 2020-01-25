package com.github.joristruong.factory

import com.github.joristruong.entity.VideoStats
import com.github.joristruong.transformer.ComputePopularityScoreTransformer
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import org.apache.spark.sql.Dataset

class PopularityScoreFactory extends Factory[Dataset[VideoStats]] {

  @Delivery(id = "videosStatsRepo")
  var videosStatsRepo: SparkRepository[VideoStats] = _
  @Delivery(id = "viewsWeight")
  var viewsWeight: Double = _
  @Delivery(id = "trendingDaysWeight")
  var trendingDaysWeight: Double = _
  @Delivery(id = "likesRatioWeight")
  var likesWeight: Double = _
  @Delivery(id = "commentsWeight")
  var commentsWeight: Double = _

  var videosStats: Dataset[VideoStats] = _

  var output: Dataset[VideoStats] = _

  override def read(): PopularityScoreFactory.this.type = {
    videosStats = videosStatsRepo.findAll()

    this
  }

  override def process(): PopularityScoreFactory.this.type = {
    output = new ComputePopularityScoreTransformer(
      videosStats,
      viewsWeight,
      trendingDaysWeight,
      likesWeight,
      commentsWeight
    ).transform().transformed

    output.show(1000)

    this
  }

  override def write(): PopularityScoreFactory.this.type = this

  override def get(): Dataset[VideoStats] = output
}
