# Youtube SETL

Youtube SETL is a project that aims at providing a starting point to practice the SETL Framework : https://github.com/JCDecaux/setl. The idea is to give a context project involving Extract, Transform and Load operations. There are three levels of difficulty for the exercise : Easy mode, Normal mode and Hard mode.

The data that is used is from Kaggle, https://www.kaggle.com/datasnaek/youtube-new.

# Installation

I used JetBrains IntelliJ IDEA Community Edition for this project, with Scala and Apache Spark.

# Context

The data is divided in multiples regions: Canada (CA), Germany (DE), France (FR), Great Britain (GB), India (IN), Japan (JP), South Korea (KR), Mexico (MX), Russia (RU) and the United States (US). For each of these regions, there are two files:
1. A CSV file, containing the following columns: <details><summary></summary>![](CSV_fields.png? "CSV File Description")</details>

Everyday, YouTube provides about 200 of the most top trending videos in each country. YouTube measures how much a video is trendy based on a combination of factors that is not made fully public. This dataset consists in a collection of everyday's top trending videos. As a consequence, it is possible for the same video to appear multiple times, meaning that it is trending for multiple days.

2. A JSON file, containing three keys:
    1. kind: String
    2. etag: String
    3. items: Array of objects

Basically, the elements of the *items* fields allow us to map the ```category_id``` of the CSV file to the full name category.

We are going to analyze this dataset and determine "popular" videos. But, how do we define a popular video ? We are going to define the popularity of a video based on its number of views, likes, dislikes, number of comments, and number of trending days.

This definition is clearly debatable and arbitrary, and we are not looking to find out the best definition for the popularity of a video. We will only focus on the purpose of this project: practice with the SETL Framework.

# Introduction

The goal of this project is to find the 100 most "popular" videos, the most "popular" categories of videos as well as the 100 most "popular" videos per category.

Below are the instructions for each difficulty level to realize the project. For each difficulty level, you can clone the repo with the specific branch to have a starting project.

For this project, we assume that you already have a basic knowledge of Scala and Apache Spark.

# General tips

* Create a folder *inputs* and move the data here.
* Try to save all the DataFrame/Dataset after each transformation, or data processing. You can have a look at them to see if there are any mistakes.
* For accomplish the tasks, you can look at *Tips* for help.

## Hard mode

### Instructions

* <details>
  <summary>Achievement 1</summary>
  
  * You are on your own ! Do whatever you please in order to achieve the tasks.

</details>


## Normal mode

### Instructions

* <details>
  <summary>Achievement 1: <b>Reading inputs</b></summary>
  
  The first thing we are going to do is, of course, reading the inputs: the CSV files, that I will call the videos files, and the JSON files, or the categories files.

  1. Let's start with the categories files. All the categories files are *JSON* files. Create a case class that represents a *Category*, then a Factory with a Transformer to will process the categories files into the case class.
        <details>
        <summary>Tips:</summary>

        * Use a **Connector** instead of a **SparkRepository**. This is mostly because it is hard to create an object that will mimic the categories files, structure-wise.
        * Take a look at the ```local.conf``` file. An object has already been created in order to read the categories files.
        * Because the files have the same structure, you can move all of them in the same folder. Setting the path to this folder, a **Connector** will consider these files as partitions of a single file.
        * We only need to select the *id* and the title of the category.
        * Try to look at the *explode* function from ```org.apache.spark.sql.functions```.
        * Do not forget to use ```coalesce``` when saving a file.

        </details>

  2. We can now work with the videos files. Similarly, create a case class that represents a *Video*, then a Factory with one or several Transformers that will do the processing. Because the videos files are separated from regions, there is not the region information for each record in the dataset. Try to add this information, and merge all the records in a single DataFrame/Dataset.

        <details>
        <summary>Tips:</summary>

        * Read the files one by one. It means to create multiple **SparkRepository** for reading.
        * Create a single **SparkRepository** for writing.
        * Select videos that are not removed or having an error.
        * Two Transformers will be useful: one for adding the ```country``` column, and one for merging all the videos into a single Dataset.

        </details>

</details>

* <details>
  <summary>Achievement 2: <b>Getting the latest videos statistics</b></summary>
  
  *

</details>

## Easy mode

### Instructions

* <details>
  <summary>Achievement 1: <b>Reading inputs</b></summary>

  *

</details>

* <details>
  <summary>Achievement 2: <b>Getting the latest videos statistics</b></summary>
  
  *

</details>


# Thanks for reading ! :heart:

If you liked this project, please check out SETL Framework here : https://github.com/JCDecaux/setl, and why not bring your contribution !
