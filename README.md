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

The goal of this project is to find the 100 most "popular" videos, and the most "popular" video categories.

Below are the instructions for each difficulty level to realize the project. For each difficulty level, you can clone the repo with the specific branch to have a starting project.

For this project, we assume that you already have a basic knowledge of Scala and Apache Spark.

# General tips

* Create a folder *inputs* in the *resources* folder and move the data here.
* The global structure of the project consists in 3 main folders: ```entity``` which contains the case classes or the objects ; ```factory``` which contains transformers ; and ```transformer``` which contains the data transformations.
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
  
  The first thing we are going to do is, of course, reading the inputs: the CSV files, that I will call the videos files, and the JSON files, the categories files.

  1. Let's start with the categories files. All the categories files are *JSON* files. Create a case class that represents a *Category*, then a Factory with a Transformer that will process the categories files into the case class.
        <details>
        <summary>Tips:</summary>

        * Use a **Connector** instead of a **SparkRepository**. This is mostly because it is hard to create an object that will mimic the categories files, structure-wise.
        * Take a look at the ```local.conf``` file. An object has already been created in order to read the categories files.
        * Because the files have the same structure, you can move all of them in the same folder. Setting the path to this folder, a **Connector** will consider these files as partitions of a single file.
        * We only need to select the *id* and the title of the category.
        * Try to look at the *explode* function from ```org.apache.spark.sql.functions```.
        * Do not forget to use ```coalesce``` when saving a file.

        </details>

  2. We can now work with the videos files. Similarly, create a case class that represents a *Video* for reading the inputs, then a Factory with one or several Transformers that will do the processing. Because the videos files are separated from regions, there is not the region information for each record in the dataset. Try to add this information by using another case class *VideoCountry* which is very similar to *Video*, and merge all the records in a single DataFrame/Dataset.

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
  
  Because a video can be a top trending one for a day and the next day, it will have different numbers in terms of views, likes, dislikes, comments... As a consequence, we have to retrieve the latest statistics available for a single video, for each region. At the same time, we are going to compute the number of trending days for every video.

  1. Create a case class *VideoStats*, that is very similar to the previous case classes, but with the trending days information. 
  2. First, compute the number of trending days of each video.

        <details>
        <summary>Tips:</summary>

        * Look at the ```window``` function from ```org.apache.spark.sql.functions```.

        </details>

  3. To retrieve the latest statistics, you have to retrieve the latest trending day of each video. It is in fact the latest available statistics.

        <details>
        <summary>Tips:</summary>

        * You will need to create another ```window```. The first one was for computing the number of trending days, and the second to retrieve the latest statistics.
        * A small trick is to use the ```rank``` function.

        </details>

  4. Sort the results by region, number of trending days, views, likes and then comments. It will prepare the data for the next achievement.

</details>

* <details>
  <summary>Achievement 3: <b>Computing the popularity score</b></summary>

  We are now going to compute the popularity score of each video, after getting their latest statistics. As said previously, our formula is very simple and may not represent the reality.

  1. Let's normalize the number of likes/dislikes over the number of views. For each record, divide the number of likes by the number of views, and then the number of dislikes by the number of views. After that, get the percentage of "normalized" likes.
  2. Let's now normalize the number of comments. For each record, divide the number of comments by the number of views.
  3. We can now compute the popularity score. The formula is going to be: ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * likesWeight + normalizedComments * commentsWeight```. <br>
  However, there are videos where comments are disabled. In this case, the formula becomes: ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * (likesWeight + commentsWeight)```. We arbitrarily decided the weights to be:
        * ```viewsWeight = 0.4```
        * ```trendingDaysWeight = 0.35```
        * ```likesWeight = 0.2```
        * ```commentsWeight = 0.05```

        Set them up as ```Input``` so they can be easily modified.

        <details><summary>Tips:</summary>

        * Check out ```when``` and ```otherwise``` functions from ```org.apache.spark.sql.functions```.

        </details>
  
  4. Sort by the ```score``` in descending order, and take the 100 first records. You now have the 100 most "popular" videos from the 10 regions.

  </details>

## Easy mode

### Instructions

* <details>
  <summary>Achievement 1: <b>Reading inputs</b></summary>

  The first thing we are going to do is, of course, reading the inputs: the CSV files, that I will call the videos files, and the JSON files, the categories files.

  1. Let's start with the categories files. All the categories files are JSON files. Here is the workflow: we are going to define a configuration file that will indicates the categories files to read ; create a case class that represents a Category ; then a Factory with a Transformer that will process the categories files into the case class. Finally, we are going to add the ```Stage``` into the ```Pipeline``` to trigger the transformations.
      
        1. <details>
            <summary>Configuration</summary>

            The configuration object has already been created in ```resources/local.conf```. Pay attention at the ```storage``` and ```path``` options. Move the categories files accordingly. If multiple files are in the same folder and the folder is used as a path, *SETL* will consider the files as partitions of a single file.

            </details>

        2. <details>
            <summary>Entity</summary>

            Create a case class named ```Category``` in the ```entity``` folder. Now examine, in the categories files, the fields that we will need.

            <details>
            <summary>Answer</summary>

            We will need the ```id``` and the ```title``` of the category.

            </details>

            </details>

        3. <details>
            <summary>Factory</summary>

             The skeleton of the Factory has already been provided. Make sure you understand the logical structure.
             * First, a ```Delivery``` in the form of a ```Connector``` allows us to retrieve the inputs. Another ```Delivery``` will act as a ```SparkRepository```, where we will write the output of the transformation. To connect with the two previous deliveries, we are going to use to other variables: one for reading the ```Connector```, and the other for storing the output.
             * Four functions are needed for a SETL ```Factory```:
                  * ```read```: the idea is to take the ```Connector``` or ```SparkRepository Delivery``` inputs, preprocess them if needed, and store them into variables to use them in the next function.
                  * ```process```: here is where all the data transformations will be done.
                  * ```write```: as its name suggests, it is used to save the output of the transformations after they have been done.
                  * ```get```: this function is used to pass the output into the next ```Stage``` of the ```Pipeline```.
            * In the ```process``` function, there can be multiple ```Transformer```. We are going to try to follow this structure throughout the rest of the project.

            <br>
            <details>
            <summary>Questions</summary>

            * <b>Why use a Connector instead of a SparkRepository ?</b><br>
            This is mostly because it is hard to create an object that will mimic the categories files, structure-wise.
            * <b>Why do you have to write the output ?</b><br>
            It is absolutely not necessary. Indeed, the result of the ```Factory``` will be automatically transferred to the next ```Stage``` through the ```get``` function. However, writing the output of every ```Factory``` will be easier for visualization and debugging.

            </details>

            </details>

        4. <details>
            <summary>Transformer</summary>

            Again, the skeleton of the Transformer has already been provided. However, you will be the one who will write the data transformation.
            * Our ```Transformer``` takes an argument. Usually, it is the DataFrame or the Dataset that we want to process. Depending on your application, you may add other arguments.
            * ```transformedData``` is the variable that will store the result of the data transformation.
            * ```transformed()``` is the method that will be called by a ```Factory``` to retrieve the result of the data transformation.
            * ```transform()``` is the method that will do the data transformations.
            * Let's now describe the transformation we want to do.
                1. First off, we are going to select the ```items``` field. If you check out the categories files, the information we need is on this field.
                2. However, the ```items``` field is an array. We want to explode this array and take only the ```id``` field and the ```title``` field from the ```snippet``` field. To do that, use the ```explode``` function from ```org.apache.spark.sql.functions```. Then, to get specific fields, use the ```withColumn``` method and the ```getField()``` method on ```id, snippet``` and ```title```. Don't forget to cast the types accordingly to the case class that you created.
                3. Select the ```id``` and the ```title``` columns. Then, cast the DataFrame into a Dataset with ```as[T]```.
            * You have finished writing the ```Transformer```. To see what it does, you can run the ```App.scala``` file that has already been created. It simply runs the ```Factory``` that contains the ```Transformer``` you just wrote, and it will output the result to the path of the configuration file.

            </details>
  

</details>

* <details>
  <summary>Achievement 2: <b>Getting the latest videos statistics</b></summary>
  
  *

</details>

* <details>
  <summary>Achievement 3: <b>Computing the popularity score</b></summary>
  
  *

</details>


# Thanks for reading ! :heart:

If you liked this project, please check out SETL Framework here : https://github.com/JCDecaux/setl, and why not bring your contribution !
