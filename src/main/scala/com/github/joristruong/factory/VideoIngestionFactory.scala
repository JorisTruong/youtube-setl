package com.github.joristruong.factory

import com.github.joristruong.entity.{Video, VideoCountry}
import com.github.joristruong.transformer.{AddCountryTransformer, MergeCountryTransformer}
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class VideoIngestionFactory extends Factory[Dataset[VideoCountry]] with HasSparkSession {

  import spark.implicits._

  @Delivery
  private[this] val videosCountryRepo = SparkRepository[VideoCountry]

  @Delivery(id = "videosCA", autoLoad = true)
  private[this] val videosCAFull = spark.emptyDataset[Video]

  @Delivery(id = "videosDE", autoLoad = true)
  private[this] val videosDEFull = spark.emptyDataset[Video]

  @Delivery(id = "videosFR", autoLoad = true)
  private[this] val videosFRFull = spark.emptyDataset[Video]

  @Delivery(id = "videosGB", autoLoad = true)
  private[this] val videosGBFull = spark.emptyDataset[Video]

  @Delivery(id = "videosIN", autoLoad = true)
  private[this] val videosINFull = spark.emptyDataset[Video]

  @Delivery(id = "videosJP", autoLoad = true)
  private[this] val videosJPFull = spark.emptyDataset[Video]

  @Delivery(id = "videosKR", autoLoad = true)
  private[this] val videosKRFull = spark.emptyDataset[Video]

  @Delivery(id = "videosMX", autoLoad = true)
  private[this] val videosMXFull = spark.emptyDataset[Video]

  @Delivery(id = "videosRU", autoLoad = true)
  private[this] val videosRUFull = spark.emptyDataset[Video]

  @Delivery(id = "videosUS", autoLoad = true)
  private[this] val videosUSFull = spark.emptyDataset[Video]

  private[this] var output: Dataset[VideoCountry] = _

  override def read(): VideoIngestionFactory.this.type = this

  override def process(): VideoIngestionFactory.this.type = {
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

  override def write(): VideoIngestionFactory.this.type = {
    videosCountryRepo.save(output.coalesce(1))
    this
  }

  override def get(): Dataset[VideoCountry] = output
}
