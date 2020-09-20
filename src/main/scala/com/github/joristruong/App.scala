package com.github.joristruong

import com.jcdecaux.setl.Setl

object App {
  def main(args: Array[String]): Unit = {
    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl
    //.setConnector(???)
    //.setSparkRepository[???](???)


    setl
      .newPipeline()
      //.setInput(???)
      .run()

  }
}
