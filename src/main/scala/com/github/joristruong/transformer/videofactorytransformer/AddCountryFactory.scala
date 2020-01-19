package com.github.joristruong.transformer.videofactorytransformer

import com.github.joristruong.entity.{Video, VideoCountry}
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

class AddCountryFactory(
                         videosRepo: SparkRepository[Video],
                         country: String
                       ) extends Transformer[Dataset[VideoCountry]] with HasSparkSession {

  import spark.implicits._

  var transformedData: Dataset[VideoCountry] = _

  override def transformed: Dataset[VideoCountry] = transformedData

  override def transform(): AddCountryFactory.this.type = {
    transformedData = videosRepo
        .findAll()
        .filter(video => !video.removed)
        .drop("removed")
        .withColumn("country", lit(country))
        .as[VideoCountry]

    this
  }
}
