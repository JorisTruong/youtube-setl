package com.github.joristruong

import com.github.joristruong.entity.{Video, VideoCountry, VideoStats}
import com.github.joristruong.factory.{LatestStatsFactory, PopularityScoreFactory, VideoIngestionFactory}
import com.jcdecaux.setl.Setl

object App {

  def main(args: Array[String]): Unit = {

    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    // Register Repositories
    setl
      .setSparkRepository[Video]("videosCARepository", deliveryId = "videosCA")
      .setSparkRepository[Video]("videosDERepository", deliveryId = "videosDE")
      .setSparkRepository[Video]("videosFRRepository", deliveryId = "videosFR")
      .setSparkRepository[Video]("videosGBRepository", deliveryId = "videosGB")
      .setSparkRepository[Video]("videosINRepository", deliveryId = "videosIN")
      .setSparkRepository[Video]("videosJPRepository", deliveryId = "videosJP")
      .setSparkRepository[Video]("videosKRRepository", deliveryId = "videosKR")
      .setSparkRepository[Video]("videosMXRepository", deliveryId = "videosMX")
      .setSparkRepository[Video]("videosRURepository", deliveryId = "videosRU")
      .setSparkRepository[Video]("videosUSRepository", deliveryId = "videosUS")
      .setSparkRepository[VideoCountry]("videosRepository")
      .setSparkRepository[VideoStats]("videosStatsRepository")

    // Instantiate pipeline
    val pipeline = setl
      .newPipeline()
      .setInput(0.5, "viewsWeight")
      .setInput(0.25, "trendingDaysWeight")
      .setInput(0.1, "likesRatioWeight")
      .setInput(0.05, "commentsWeight")

    // Register Stages
    pipeline
      .addStage[VideoIngestionFactory]()
      .addStage[LatestStatsFactory]()
      .addStage[PopularityScoreFactory]()
      .describe()

    // Display Diagram
    pipeline.showDiagram()

    // Run the pipeline
    pipeline.run()

  }

}
