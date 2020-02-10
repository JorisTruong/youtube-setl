package com.github.joristruong.factory

import com.github.joristruong.entity.{Video, VideoCountry}
import com.github.joristruong.transformer.videofactorytransformer.{AddCountryTransformer, MergeCountryTransformer}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class VideoFactory extends Factory[Dataset[VideoCountry]] with HasSparkSession {

  import spark.implicits._

  @Delivery(id = "videosCARepo", autoLoad = true) private[this] val videosCAFull = spark.emptyDataset[Video]
  @Delivery(id = "videosDERepo", autoLoad = true) private[this] val videosDEFull = spark.emptyDataset[Video]
  @Delivery(id = "videosFRRepo", autoLoad = true) private[this] val videosFRFull = spark.emptyDataset[Video]
  @Delivery(id = "videosGBRepo", autoLoad = true) private[this] val videosGBFull = spark.emptyDataset[Video]
  @Delivery(id = "videosINRepo", autoLoad = true) private[this] val videosINFull = spark.emptyDataset[Video]
  @Delivery(id = "videosJPRepo", autoLoad = true) private[this] val videosJPFull = spark.emptyDataset[Video]
  @Delivery(id = "videosKRRepo", autoLoad = true) private[this] val videosKRFull = spark.emptyDataset[Video]
  @Delivery(id = "videosMXRepo", autoLoad = true) private[this] val videosMXFull = spark.emptyDataset[Video]
  @Delivery(id = "videosRURepo", autoLoad = true) private[this] val videosRUFull = spark.emptyDataset[Video]
  @Delivery(id = "videosUSRepo", autoLoad = true) private[this] val videosUSFull = spark.emptyDataset[Video]

  @Delivery private[this] val videosCountryRepo = SparkRepository[VideoCountry]

  private[this] var output: Dataset[VideoCountry] = _

  override def read(): VideoFactory.this.type = this

  override def process(): VideoFactory.this.type = {
    val allVideos = Seq(
      (videosCAFull, "CA"),
      (videosDEFull, "DE"),
      (videosFRFull, "FR"),
      (videosGBFull, "GB"),
      (videosINFull, "IN"),
      (videosJPFull, "JP"),
      (videosKRFull, "KR"),
      (videosMXFull, "MX"),
      (videosRUFull, "RU"),
      (videosUSFull, "US")
    ).map {
      case (data, country) => new AddCountryTransformer(data, country).transform().transformed
    }

    output = new MergeCountryTransformer(allVideos).transform().transformed
    this
  }

  override def write(): VideoFactory.this.type = {
    videosCountryRepo.save(output.coalesce(1))
    this
  }

  override def get(): Dataset[VideoCountry] = output
}
