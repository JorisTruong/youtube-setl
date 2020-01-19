package com.github.joristruong.transformer.videofactorytransformer

import com.github.joristruong.entity.VideoCountry
import com.jcdecaux.setl.transformation.Transformer
import org.apache.spark.sql.Dataset

class MergeCountryFactory(allVideos: Seq[Dataset[VideoCountry]]) extends Transformer[Dataset[VideoCountry]] {

  var transformedData: Dataset[VideoCountry] = _

  override def transformed: Dataset[VideoCountry] = transformedData

  override def transform(): MergeCountryFactory.this.type = {
    transformedData = allVideos.reduce(_.union(_))

    this
  }
}
