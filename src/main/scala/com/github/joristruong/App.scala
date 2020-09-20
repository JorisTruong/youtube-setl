package com.github.joristruong

import com.github.joristruong.entity.Category
import com.github.joristruong.factory.CategoryFactory
import com.jcdecaux.setl.Setl

object App {
  def main(args: Array[String]): Unit = {
    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl
      .setConnector("categoriesReadConnector", deliveryId = "categoriesReadConn")
      .setSparkRepository[Category]("categoriesWriteRepository", deliveryId = "categoriesWriteRepo")

    setl
      .newPipeline()
      //.setInput[???](???)
      .addStage[CategoryFactory]()
      .run()

  }
}
