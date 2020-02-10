package com.github.joristruong.factory

import com.github.joristruong.entity.{VideoCountry, VideoStats}
import com.github.joristruong.transformer.StatsTransformer
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class LatestStatsFactory extends Factory[Dataset[VideoStats]] with HasSparkSession {

  import spark.implicits._

  @Delivery(autoLoad = true)
  private[this] val videos = spark.emptyDataset[VideoCountry]

  @Delivery
  private[this] val videosStatsRepo = SparkRepository[VideoStats]

  var output: Dataset[VideoStats] = _

  override def read(): LatestStatsFactory.this.type = this

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
