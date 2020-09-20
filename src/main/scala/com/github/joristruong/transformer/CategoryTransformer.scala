package com.github.joristruong.transformer

import com.github.joristruong.entity.Category
import com.jcdecaux.setl.transformation.Transformer
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset


class CategoryTransformer extends Transformer[Dataset[Category]] with HasSparkSession {

  override def transformed: Dataset[Category] = ???

  override def transform(): CategoryTransformer.this.type = ???
}
