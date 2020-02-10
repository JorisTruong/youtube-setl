package com.github.joristruong.transformer

import com.github.joristruong.entity.VideoCountry
import com.jcdecaux.setl.transformation.Transformer
import org.apache.spark.sql.Dataset

class MergeCountryTransformer(allVideos: Seq[Dataset[VideoCountry]]) extends Transformer[Dataset[VideoCountry]] {

  var transformedData: Dataset[VideoCountry] = _

  override def transformed: Dataset[VideoCountry] = transformedData

  override def transform(): MergeCountryTransformer.this.type = {
    transformedData = allVideos.reduce(_.union(_))

    this
  }
}
