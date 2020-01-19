package com.github.joristruong.factory

import com.github.joristruong.entity.{Video, VideoCountry}
import com.github.joristruong.transformer.videofactorytransformer.{AddCountryFactory, MergeCountryFactory}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class VideoFactory extends Factory[Dataset[VideoCountry]] with HasSparkSession {
  import spark.implicits._

  @Delivery(id = "videosCARepo")
  var videosCARepo: SparkRepository[Video] = _
  @Delivery(id = "videosDERepo")
  var videosDERepo: SparkRepository[Video] = _
  @Delivery(id = "videosFRRepo")
  var videosFRRepo: SparkRepository[Video] = _
  @Delivery(id = "videosGBRepo")
  var videosGBRepo: SparkRepository[Video] = _
  @Delivery(id = "videosINRepo")
  var videosINRepo: SparkRepository[Video] = _
  @Delivery(id = "videosJPRepo")
  var videosJPRepo: SparkRepository[Video] = _
  @Delivery(id = "videosKRRepo")
  var videosKRRepo: SparkRepository[Video] = _
  @Delivery(id = "videosMXRepo")
  var videosMXRepo: SparkRepository[Video] = _
  @Delivery(id = "videosRURepo")
  var videosRURepo: SparkRepository[Video] = _
  @Delivery(id = "videosUSRepo")
  var videosUSRepo: SparkRepository[Video] = _
  @Delivery(id = "videosRepo")
  var videosCountryRepo: SparkRepository[VideoCountry] = _

  var videos: Dataset[Video] = _

  var output: Dataset[VideoCountry] = _

  override def read(): VideoFactory.this.type = this

  override def process(): VideoFactory.this.type = {
    val videosCA = new AddCountryFactory(videosCARepo, "CA").transform().transformed
    val videosDE = new AddCountryFactory(videosDERepo, "DE").transform().transformed
    val videosFR = new AddCountryFactory(videosFRRepo, "FR").transform().transformed
    val videosGB = new AddCountryFactory(videosGBRepo, "GB").transform().transformed
    val videosIN = new AddCountryFactory(videosINRepo, "IN").transform().transformed
    val videosJP = new AddCountryFactory(videosJPRepo, "JP").transform().transformed
    val videosKR = new AddCountryFactory(videosKRRepo, "KR").transform().transformed
    val videosMX = new AddCountryFactory(videosMXRepo, "MX").transform().transformed
    val videosRU = new AddCountryFactory(videosRURepo, "RU").transform().transformed
    val videosUS = new AddCountryFactory(videosUSRepo, "US").transform().transformed

    val allVideos = Seq(
      videosCA,
      videosDE,
      videosFR,
      videosGB,
      videosIN,
      videosJP,
      videosKR,
      videosMX,
      videosRU,
      videosUS
    )

    output = new MergeCountryFactory(allVideos).transform().transformed

    this
  }

  override def write(): VideoFactory.this.type = {
    videosCountryRepo.save(output.coalesce(1))

    this
  }

  override def get(): Dataset[VideoCountry] = output
}
