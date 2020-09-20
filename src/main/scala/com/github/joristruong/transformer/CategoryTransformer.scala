package com.github.joristruong.transformer

import com.github.joristruong.entity.Category
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}


class CategoryTransformer(categories: DataFrame) extends Transformer[Dataset[Category]] with HasSparkSession {

  import spark.implicits._

  var transformedData: Dataset[Category] = _

  override def transformed: Dataset[Category] = transformedData

  override def transform(): CategoryTransformer.this.type = {
    transformedData = categories
      //.select(explode($"???") as "item")
      .withColumn("???", $"???".getField("???").cast("???"))
      .withColumn("???", $"???".getField("???").getField("???"))
      .select("???", "???")
      .distinct()
      .as[Category]

    this
  }
}
