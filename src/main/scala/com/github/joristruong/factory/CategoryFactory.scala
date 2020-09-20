package com.github.joristruong.factory

import com.github.joristruong.entity.Category
import com.github.joristruong.transformer.CategoryTransformer
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.storage.connector.Connector
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.{DataFrame, Dataset}

class CategoryFactory extends Factory[Dataset[Category]] with HasSparkSession {

  @Delivery(id = "categoriesReadConn")
  var categoriesReadConn: Connector = _
  @Delivery(id = "categoriesWriteRepo")
  var categoriesWriteRepo: SparkRepository[Category] = _

  var categories: DataFrame = _

  var output: Dataset[Category] = _

  override def read(): CategoryFactory.this.type = {
    categories = categoriesReadConn.read()

    this
  }

  override def process(): CategoryFactory.this.type = {
    output = new CategoryTransformer(categories).transform().transformed

    this
  }

  override def write(): CategoryFactory.this.type = {
    categoriesWriteRepo.save(output.coalesce(1))

    this
  }

  override def get(): Dataset[Category] = output
}
