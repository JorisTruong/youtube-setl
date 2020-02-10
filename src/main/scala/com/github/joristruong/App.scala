package com.github.joristruong

import com.github.joristruong.entity.{Video, VideoCountry, VideoStats}
import com.github.joristruong.factory.{LatestStatsFactory, VideoFactory}
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
      .addStage[VideoFactory]()
      .addStage[LatestStatsFactory]()
      .describe()

    pipeline.showDiagram()
    pipeline.run()

  }

}
