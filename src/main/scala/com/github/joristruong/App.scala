package com.github.joristruong

import com.github.joristruong.entity.{Video, VideoCountry, VideoStats}
import com.github.joristruong.factory.{LatestStatsFactory, PopularityScoreFactory, VideoFactory}
import com.jcdecaux.setl.Setl

object App {

  def main(args: Array[String]): Unit = {

    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    // Register Repositories
    setl
      .setSparkRepository[Video]("videosCARepository", deliveryId = "videosCARepo")
      .setSparkRepository[Video]("videosDERepository", deliveryId = "videosDERepo")
      .setSparkRepository[Video]("videosFRRepository", deliveryId = "videosFRRepo")
      .setSparkRepository[Video]("videosGBRepository", deliveryId = "videosGBRepo")
      .setSparkRepository[Video]("videosINRepository", deliveryId = "videosINRepo")
      .setSparkRepository[Video]("videosJPRepository", deliveryId = "videosJPRepo")
      .setSparkRepository[Video]("videosKRRepository", deliveryId = "videosKRRepo")
      .setSparkRepository[Video]("videosMXRepository", deliveryId = "videosMXRepo")
      .setSparkRepository[Video]("videosRURepository", deliveryId = "videosRURepo")
      .setSparkRepository[Video]("videosUSRepository", deliveryId = "videosUSRepo")
      .setSparkRepository[VideoCountry]("videosRepository")
      .setSparkRepository[VideoStats]("videosStatsRepository")

    // Register Stages
    val pipeline = setl
      .newPipeline()
      .setInput(0.5, "viewsWeight")
      .setInput(0.25, "trendingDaysWeight")
      .setInput(0.1, "likesRatioWeight")
      .setInput(0.05, "commentsWeight")
      .addStage[VideoFactory]()
      .addStage[LatestStatsFactory]()
      .addStage[PopularityScoreFactory]()
      .describe()

    pipeline.showDiagram()
//    pipeline.run()

  }

}
