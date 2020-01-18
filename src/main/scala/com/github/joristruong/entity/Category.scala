package com.github.joristruong.entity

import com.jcdecaux.setl.annotation.ColumnName

case class Category(
                     @ColumnName("category_id") categoryId: Long,
                     @ColumnName("category_title") categoryTitle: String
                   )
