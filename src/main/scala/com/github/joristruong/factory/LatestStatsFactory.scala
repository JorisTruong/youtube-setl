package com.github.joristruong.factory

import com.github.joristruong.entity.{Video, VideoCountry, VideoStats}
import com.github.joristruong.transformer.StatsTransformer
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class LatestStatsFactory extends Factory[Dataset[VideoStats]] with HasSparkSession {
  import spark.implicits._

  @Delivery(id = "videosRepo")
  var videosRepo: SparkRepository[VideoCountry] = _
  @Delivery(id = "videosStatsRepo")
  var videosStatsRepo: SparkRepository[VideoStats] = _

  var videos: Dataset[VideoCountry] = _

  var output: Dataset[VideoStats] = _

  override def read(): LatestStatsFactory.this.type = {
    videos = videosRepo
      .findAll()

    this
  }

  override def process(): LatestStatsFactory.this.type = {
    output = new StatsTransformer(videos).transform().transformed

    this
  }

  override def write(): LatestStatsFactory.this.type = {
    videosStatsRepo.save(output.coalesce(1))

    this
  }

  override def get(): Dataset[VideoStats] = output
}
