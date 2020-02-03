package com.github.joristruong.factory

import com.github.joristruong.entity.{Video, VideoCountry}
import com.github.joristruong.transformer.videofactorytransformer.{AddCountryFactory, MergeCountryFactory}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import org.apache.spark.sql.Dataset

class VideoFactory extends Factory[Dataset[VideoCountry]] {
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

  var videosCAFull: Dataset[Video] = _
  var videosDEFull: Dataset[Video] = _
  var videosFRFull: Dataset[Video] = _
  var videosGBFull: Dataset[Video] = _
  var videosINFull: Dataset[Video] = _
  var videosJPFull: Dataset[Video] = _
  var videosKRFull: Dataset[Video] = _
  var videosMXFull: Dataset[Video] = _
  var videosRUFull: Dataset[Video] = _
  var videosUSFull: Dataset[Video] = _

  var videos: Dataset[Video] = _

  var output: Dataset[VideoCountry] = _

  override def read(): VideoFactory.this.type = {
    videosCAFull = videosCARepo.findAll()
    videosDEFull = videosDERepo.findAll()
    videosFRFull = videosFRRepo.findAll()
    videosGBFull = videosGBRepo.findAll()
    videosINFull = videosINRepo.findAll()
    videosJPFull = videosJPRepo.findAll()
    videosKRFull = videosKRRepo.findAll()
    videosMXFull = videosMXRepo.findAll()
    videosRUFull = videosRURepo.findAll()
    videosUSFull = videosUSRepo.findAll()

    this
  }

  override def process(): VideoFactory.this.type = {
    val videosCA = new AddCountryFactory(videosCAFull, "CA").transform().transformed
    val videosDE = new AddCountryFactory(videosDEFull, "DE").transform().transformed
    val videosFR = new AddCountryFactory(videosFRFull, "FR").transform().transformed
    val videosGB = new AddCountryFactory(videosGBFull, "GB").transform().transformed
    val videosIN = new AddCountryFactory(videosINFull, "IN").transform().transformed
    val videosJP = new AddCountryFactory(videosJPFull, "JP").transform().transformed
    val videosKR = new AddCountryFactory(videosKRFull, "KR").transform().transformed
    val videosMX = new AddCountryFactory(videosMXFull, "MX").transform().transformed
    val videosRU = new AddCountryFactory(videosRUFull, "RU").transform().transformed
    val videosUS = new AddCountryFactory(videosUSFull, "US").transform().transformed

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
