package com.github.joristruong.transformer

import com.github.joristruong.entity.{VideoCountry, VideoStats}
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class StatsTransformer(videos: Dataset[VideoCountry]) extends Transformer[Dataset[VideoStats]] with HasSparkSession {

  import spark.implicits._

  var transformedData: Dataset[VideoStats] = _

  override def transformed: Dataset[VideoStats] = transformedData

  override def transform(): StatsTransformer.this.type = {
    val w = Window
      .partitionBy(
        "videoId",
        "title",
        "channelTitle",
        "categoryId",
        "commentDisabled",
        "country"
      )

    val w2 = w.orderBy($"trendingDate".desc)

    transformedData = videos
      .withColumn("trendingDays", count($"videoId").over(w).cast("int"))
      .withColumn("rank", rank().over(w2))
      .filter(row => row.getAs[Int]("rank") == 1)
      .drop("rank")
      .sort($"country", $"trendingDays".desc, $"views".desc, $"likes".desc)
      .as[VideoStats]

    this
  }
}
