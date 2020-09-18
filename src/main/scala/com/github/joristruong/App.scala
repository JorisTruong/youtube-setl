package com.github.joristruong

import com.github.joristruong.entity.{Category, Video, VideoCountry, VideoStats}
import com.github.joristruong.factory.{CategoryFactory, LatestStatsFactory, PopularityScoreFactory, VideoFactory}
import com.jcdecaux.setl.Setl

object App {
  def main(args: Array[String]): Unit = {
    val setl = Setl.builder()
      .withDefaultConfigLoader()
      .getOrCreate()

    setl
      .setConnector("categoriesReadConnector", deliveryId = "categoriesReadConn")
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

      .setSparkRepository[Category]("categoriesWriteRepository", deliveryId = "categoriesWriteRepo")
      .setSparkRepository[VideoCountry]("videosRepository", deliveryId = "videosRepo")
      .setSparkRepository[VideoStats]("videosStatsRepository", deliveryId = "videosStatsRepo")
      .setSparkRepository[VideoStats]("sortedVideosStatsRepository", deliveryId = "sortedVideosStatsRepo")


    setl
      .newPipeline()
      .setInput[Double](0.4, "viewsWeight")
      .setInput[Double](0.35, "trendingDaysWeight")
      .setInput[Double](0.2, "likesRatioWeight")
      .setInput[Double](0.05, "commentsWeight")
      .addStage[CategoryFactory]()
      .addStage[VideoFactory]()
      .addStage[LatestStatsFactory]()
      .addStage[PopularityScoreFactory]()
      .run()

  }
}
