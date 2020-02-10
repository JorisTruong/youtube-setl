package com.github.joristruong.entity

import java.sql.Date

import com.jcdecaux.setl.annotation.ColumnName

case class VideoCountry(@ColumnName("video_id") videoId: String,
                        title: String,
                        @ColumnName("channel_title") channelTitle: String,
                        @ColumnName("category_id") categoryId: String,
                        @ColumnName("trending_date") trendingDate: Date,
                        views: Long,
                        likes: Long,
                        dislikes: Long,
                        @ColumnName("comment_count") commentCount: Long,
                        @ColumnName("comments_disabled") commentDisabled: Boolean,
                        country: String)
