package com.github.joristruong.factory

import com.github.joristruong.entity.Category
import com.jcdecaux.setl.transformation.Factory
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.Dataset

class CategoryFactory extends Factory[Dataset[Category]] with HasSparkSession {

  override def read(): CategoryFactory.this.type = ???

  override def process(): CategoryFactory.this.type = ???

  override def write(): CategoryFactory.this.type = ???

  override def get(): Dataset[Category] = ???
}
