package com.github.joristruong.entity

import java.sql.Timestamp
import com.jcdecaux.setl.annotation.ColumnName

case class Video(
                  @ColumnName("video_id") videoId: String,
                  title: String,
                  @ColumnName("channel_title") channelTitle: String,
                  @ColumnName("category_id") categoryId: String,
                  @ColumnName("publish_time") publishTime: Timestamp,
                  views: Long,
                  likes: Long,
                  dislikes: Long,
                  @ColumnName("comment_count") commentCount: Long,
                  @ColumnName("comments_disabled") commentDisabled: Boolean,
                  @ColumnName("ratings_disabled") ratingsDisabled: Boolean,
                  @ColumnName("video_error_or_removed") inUse: Boolean
                )
