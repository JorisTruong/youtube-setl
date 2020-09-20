# Youtube SETL

Youtube SETL is a project that aims at providing a starting point to practice the SETL Framework: https://github.com/SETL-Developers/setl. The idea is to give a context project involving Extract, Transform and Load operations. There are three levels of difficulty for the exercise: Easy mode, Normal mode and Hard mode.

The data that is used is from Kaggle, https://www.kaggle.com/datasnaek/youtube-new.

# Installation

I used JetBrains IntelliJ IDEA Community Edition for this project, with Scala and Apache Spark.

# Context

The data is divided in multiples regions: Canada (CA), Germany (DE), France (FR), Great Britain (GB), India (IN), Japan (JP), South Korea (KR), Mexico (MX), Russia (RU) and the United States (US). For each of these regions, there are two files:
1. A CSV file, containing the following columns: <details><summary></summary>![](CSV_fields.png? "CSV File Description")</details>

Everyday, YouTube provides about 200 of the most trending videos in each country. YouTube measures how much a video is trendy based on a combination of factors that is not made fully public. This dataset consists in a collection of everyday's top trending videos. As a consequence, it is possible for the same video to appear multiple times, meaning that it is trending for multiple days.

2. A JSON file, containing three keys:
    1. kind: String
    2. etag: String
    3. items: Array of objects

Basically, the elements of the *items* fields allow us to map the ```category_id``` of the CSV file to the full name category.

We are going to analyze this dataset and determine "popular" videos. But, how do we define a popular video ? We are going to define the popularity of a video based on its number of views, likes, dislikes, number of comments, and number of trending days.

This definition is clearly debatable and arbitrary, and we are not looking to find out the best definition for the popularity of a video. We will only focus on the purpose of this project: practice with the SETL Framework.

# Introduction

The goal of this project is to find the 100 most "popular" videos, and the most "popular" video categories. But how do we defined the popularity of a video ? The formula is going to be: <br>```number of views * views weight + number of trending days * trending days weight + normalized likes percentage * likes weight + normalized comments * comments weight```. <br> The likes percentage is the ratio of likes over dislikes. This ratio is normalized over the number of views. The same normalization is done with the number of comments.

Below are the instructions for each difficulty level to realize the project. For each difficulty level, you can clone the repo with the specific branch to have a starting project.

For this project, we assume that you already have a basic knowledge of Scala and Apache Spark.

# General tips

* Create a folder *inputs* in the *resources* folder and move the data here.
* The global structure of the project consists in 3 main folders: ```entity``` which contains the case classes or the objects ; ```factory``` which contains transformers ; and ```transformer``` which contains the data transformations.
* Try to save all the DataFrame/Dataset after each transformation, or data processing. You can have a look at them to see if there are any mistakes.
* To accomplish the tasks, you can look at *Tips* for help.
* If you use IntelliJ IDEA, when you create a _SETL_ ```Factory``` or ```Transformer```, you can use ```Ctrl+i``` to automatically create the needed functions. 

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
  
  The first thing we are going to do is, of course, read the inputs: the CSV files, that I will call the videos files, and the JSON files, the categories files.

  1. Let's start with the categories files. All the categories files are *JSON* files. Create a case class that represents a *Category*, then a ```Factory``` with a ```Transformer``` that will process the categories files into the case class.
        <details>
        <summary>Tips:</summary>

        * Use a **Connector** instead of a **SparkRepository**. This is mostly because it is hard to create an object that will mimic the categories files, structure-wise.
        * Take a look at the ```local.conf``` file. An object has already been created in order to read the categories files.
        * Because the files have the same structure, you can move all of them in the same folder. Setting the path to this folder, a **Connector** will consider these files as partitions of a single file.
        * We only need to select the *id* and the title of the category.
        * Try to look at the *explode* function from ```org.apache.spark.sql.functions```.
        * Do not forget to use ```coalesce``` when saving a file.

        </details>

  2. We can now work with the videos files. Similarly, create a case class that represents a *Video* for reading the inputs, then a ```Factory``` with one or several ```Transformers``` that will do the processing. Because the videos files are separated from regions, there is not the region information for each record in the dataset. Try to add this information by using another case class *VideoCountry* which is very similar to *Video*, and merge all the records in a single DataFrame/Dataset.

        <details>
        <summary>Tips:</summary>

        * Read the files one by one. It means to create multiple **SparkRepository** for reading.
        * Create a single **SparkRepository** for writing.
        * Select videos that are not removed or having an error.
        * Two ```Transformers``` will be useful: one for adding the ```country``` column, and one for merging all the videos into a single Dataset.

        </details>

</details>

* <details>
  <summary>Achievement 2: <b>Getting the latest videos statistics</b></summary>
  
  Because a video can be a top trending one for a day and the next day, it is possible for a video to have multiple rows, where each have different numbers in terms of views, likes, dislikes, comments... As a consequence, we have to retrieve the latest statistics available for a single video, for each region, because these statistics are incremental. At the same time, we are going to compute the number of trending days for every video.

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
  3. We can now compute the popularity score. Remind that the formula is: ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * likesWeight + normalizedComments * commentsWeight```. <br>
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

  1. Let's start with the categories files. All the categories files are JSON files. Here is the workflow: we are going to define a configuration file that will indicates the categories files to read ; create a case class that represents a Category ; then a ```Factory``` with a ```Transformer``` that will process the categories files into the case class. Finally, we are going to add the ```Stage``` into the ```Pipeline``` to trigger the transformations.
      
        1. <details>
            <summary>Configuration</summary>

            The configuration object has already been created in ```resources/local.conf```. Pay attention at the ```storage``` and ```path``` options. Move the categories files accordingly. If multiple files are in the same folder and the folder is used as a path, _SETL_ will consider the files as partitions of a single file. Next, check out the ```App.scala```. You can see that we used the ```setConnector()``` and ```setSparkRepository()``` methods. Every time you want to use a repository, you will need to add a configuration in the configuration and register it in the ```setl``` object.

            </details>

        2. <details>
            <summary>Entity</summary>

            Create a case class named ```Category``` in the ```entity``` folder. Now examine, in the categories files, the fields that we will need.

            <details>
            <summary>Answer</summary>

            We will need the ```id``` and the ```title``` of the category. Make sure to check the files and use the same spelling to create the ```Category``` case class.

            </details>

            </details>

        3. <details>
            <summary>Factory</summary>

             The skeleton of the ```Factory``` has already been provided. Make sure you understand the logical structure.
             * First, a ```Delivery``` in the form of a ```Connector``` allows us to retrieve the inputs. Another ```Delivery``` will act as a ```SparkRepository```, where we will write the output of the transformation. Check out the ```id``` of each ```Delivery``` and the ```deliveryId``` in ```App.scala```. They are used so there are no ambiguity when _SETL_ fetch the repositories. To be able to read the two previous deliveries, we are going to use two other variables: a ```DataFrame``` for reading the ```Connector```, and a ```Dataset``` for storing the output ```SparkRepository```. The difference between them is that a ```SparkRepository``` is typed, hence the ```Dataset```.
             * Four functions are needed for a _SETL_ ```Factory```:
                  * ```read```: the idea is to take the ```Connector``` or ```SparkRepository Delivery``` inputs, preprocess them if needed, and store them into variables to use them in the next function.
                  * ```process```: here is where all the data transformations will be done. Create an instance of the ```Transformer``` you are using, call the ```transform()``` method, use the ```transformed``` getter and store the result into a variable.
                  * ```write```: as its name suggests, it is used to save the output of the transformations after they have been done. A ```Connector``` uses the ```write()``` method to save a ```DataFrame```, and a ```SparkRepository``` uses the ```save()``` method to save a ```Dataset```.
                  * ```get```: this function is used to pass the output into the next ```Stage``` of the ```Pipeline```. Just return the ```Dataset```.
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

            Again, the skeleton of the ```Transformer``` has already been provided. However, you will be the one who will write the data transformation.
            * Our ```Transformer``` takes an argument. Usually, it is the ```DataFrame``` or the ```Dataset``` that we want to process. Depending on your application, you may add other arguments.
            * ```transformedData``` is the variable that will store the result of the data transformation.
            * ```transformed``` is the getter that will be called by a ```Factory``` to retrieve the result of the data transformation.
            * ```transform()``` is the method that will do the data transformations.
            * Let's now describe the transformation we want to do.
                1. First off, we are going to select the ```items``` field. If you check out the categories files, the information we need is on this field.
                2. However, the ```items``` field is an array. We want to explode this array and take only the ```id``` field and the ```title``` field from the ```snippet``` field. To do that, use the ```explode``` function from ```org.apache.spark.sql.functions```. Then, to get specific fields, use the ```withColumn``` method and the ```getField()``` method on ```id, snippet``` and ```title```. Don't forget to cast the types accordingly to the case class that you created.
                3. Select the ```id``` and the ```title``` columns. Then, cast the DataFrame into a Dataset with ```as[T]```.
            * You have finished writing the ```Transformer```. To see what it does, you can run the ```App.scala``` file that has already been created. It simply runs the ```Factory``` that contains the ```Transformer``` you just wrote, and it will output the result to the path of the configuration file. Note that the corresponding ```Factory``` has been added via ```addStage()``` that makes the ```Pipeline``` run it.

            </details>
    
    <br>
    <details>
    <summary>What you should know by now</summary>

    * The general structure: config, entity, transformer, factory, and finally stage in the pipeline.
    * Read JSON files.
    * How to read inputs: creating a configuration object, setting up a ```Connector```, using the ```@Delivery``` annotation, with ```deliveryId```.
    * _SETL_ can read partitions by setting a folder path in the configuration object.
    * Where to process data: using ```Transformer``` in the ```process``` method of a ```Factory```.
    * How to write output: with the ```write``` method of a ```Factory```.

    </details>
  
  2. Let's now process the videos files. We would like to merge all the files in a single ```DataFrame```/```Dataset``` or in the same CSV file, while keeping the information of the region for each video. All videos files are CSV files and they have the same columns, as previously stated in the **Context** section. The workflow is similar to the last one:  configuration ; case class ; ```Factory``` ; ```Transformer``` ; add the ```Stage``` into the ```Pipeline```. This time, we are going to set multiple configuration objects.

        1. <details>
            <summary>Configuration</summary>

            We are going to set multiple configuration objects in ```resources/local.conf```, one per region. In each configuration object, you will have to set ```storage, path, inferSchema, delimiter, header, multiLine``` and ```dateFormat```.

            <details>
            <summary>Tips</summary>

            * For these configuration files, try to give a generic name, like ```videos<region>Repository```.
            * Do not forget to set a configuration object for writing the output of the ```Factory```.

            </details>

            <details>
            <summary>Questions</summary>

            * <b>Why do we have to set multiple configuration objects, and not use a single object and move all the files in the same folder, similar to the categories files ?</b><br>
            This is because we need to keep the region information. For each of the region videos files, we will have to add a column containing the region. If we used a single object and treated all the files as partitions of a single file, we would not be able to write different regions.

            </details>
            
        2. <details>
            <summary>Entity</summary>

            Create a case class named ```Video``` in the ```entity``` folder. Now examine, in the videos files, the fields that we will need. Remind that the objective is to compute the popularity score, and that the formula is ```number of views * views weight + number of trending days * trending days weight + normalized likes percentage * likes weight + normalized comments * comments weight```. It will help to select the fields.

            Create another case class named ```VideoCountry```. It will have exactly the same fields as ```Video```, but with the country/region field in addition.

            <details>
            <summary>Tips</summary>

            * You can look at ```@ColumnName``` annotation of the framework. Try to use it as it can be useful in some  real-life business situations.
            * Use ```java.sql.Date``` for a date type field.

            </details>
            <details>
            <summary>Answer</summary>

            We would like to have the ```videoId```, ```title```, ```channel_title```, ```category_id```, ```trending_date```, ```views```, ```likes```, ```dislikes```, ```comment_count```, ```comments_disabled``` and ```video_error_or_removed``` fields.

            </details>

            </details>

        3. <details>
            <summary>Factory</summary>

             The goal of this factory is to merge all the videos files into a single one, without removing the region information. That means that we are going to use two kind of ```Transformer```.
             * First of all, set all the inputs ```Delivery``` in the form of a ```SparkRepository[Video]```. Set a last ```Delivery``` as a ```SparkRepository[VideoCountry]```, where we will write the output of the transformation. Set as many variables ```Dataset[Video]``` as the number of inputs.
             * Let's now describe the four functions of the ```Factory```:
                  * ```read```: preprocess the ```SparkRepository``` by filtering the videos that are _removed_ or _error_. Then, "cast" them as ```Dataset[Video]``` and store them into the corresponding variables.
                  * ```process```: Apply the first ```Transformer``` for each of the inputs, and apply the results to the second ```Transformer```.
                  * ```write```: write the output ```SparkRepository[VideoCountry]```.
                  * ```get```: just return the result of the final ```Transformer```.

            <br>
            <details>
            <summary>Questions</summary>

            * **Why didn't we use a ```Connector``` to read the input files and a ```SparkRepository``` for the output ?**<br>
            You can totally do that! Feel free to do that if you prefer this way. We used ```SparkRepository``` to read the inputs just to provide a structure for the input files.
            * **I feel like there is a lot of ```SparkRepository``` and a lot of corresponding variables, and I don't find this pretty/consise. Isn't there another solution ?**<br>
            Instead of using ```Delivery``` in the form of a ```SparkRepository```, you can use deliveries in the form of a ```Dataset``` with ```autoLoad = true``` option. So, instead of having:
                ```
                @Delivery(id = "id")
                var videosRegionRepo: SparkRepository[Video] = _
                var videosRegion: Dataset[Video]
                ```
                you can use:
                ```
                @Delivery(id = "id", autoLoad = true)
                var videosRegion: Dataset[Video]
                ```
                Do not hesitate the check the SETL wiki, in the Annotation section.

            </details>

            </details>

        4. <details>
            <summary>Transformer</summary>

            1. The main goal of the first ```Transformer``` is to add the region/country information. Build a ```Transformer``` that takes two inputs, a ```Dataset[Video]``` and a string. Add the column ```country``` and return a ```Dataset[VideoCountry]```. You can also filter the videos that are labeled as *removed or error*. Of course, this last step can be placed elsewhere.

            2. The main goal of the second ```Transformer``` is to regroup all the videos together, while keeping the region information.

                <details>
                <summary>Tips</summary>

                * Use ```reduce``` and ```union``` functions.

                </details>

            </details>
            
      To check the result of your work, go to ```App.scala```, set the ```SparkRepositories```, add the stage ```VideoFactory```, and run the code. It will create the output file in the corresponding path.

    <br>
    <details>
    <summary>What you should know by now</summary>

    * Read CSV files.
    * Use both ```Connector``` and ```SparkRepository```.
    * Read multiple ```Deliveries``` into a ```Transformer``` or a ```Connector```.
    * Use multiple ```Transformers``` in a ```Factory```.

    </details>

    </details>

</details>

* <details>
  <summary>Achievement 2: <b>Getting the latest videos statistics</b></summary>

  Because a video can be a top trending one for a day and the next day, it will have different numbers in terms of views, likes, dislikes, comments... As a consequence, we have to retrieve the latest statistics available for a single video, for each region. At the same time, we are going to compute the number of trending days for every video.

  But how are we going to do that ? First of all, we are going to group the records that correspond to the same video, and count the number of records, which is basically the number of trending days. Then, we are going to rank these grouped records and take the latest one, to retrieve the latest statistics.
  
  1. <details>
        <summary>Configuration</summary>

        The configuration file for the output of ```VideoFactory``` is already set in the previous achievement so it can be saved. You will need to read it and process it to get the latest videos statistics. Do not forget to add a configuration file for the output of this new ```Factory```.

        </details>
        
  2. <details>
        <summary>Entity</summary>

        Create a case class named ```VideoStats``` which have similar fields to ```VideoCountry```, but you need to take into account the number of trending days.

        </details>

  3. <details>
        <summary>Factory</summary>

        In this factory, all you need to do is to read the input, pass it to the ```Transformer``` that will do the data processing, and write the output. It should be pretty simple; you can try to imitate the other ```Factories```.

        <details>
        <summary>Tips</summary>

        * Do not forget to set the inputs and outputs ```Deliveries```.

        </details>

        </details>

  4. <details>
        <summary>Transformer</summary>

        As previously said, we are going to group the videos together. For that, we are going to use ```org.apache.spark.sql.expressions.Window```. Make sure you know what a ```Window``` does beforehand.

        1. Create a first ```Window``` that you will partition by for counting the number of trending days for each video. To know which fields you are going to partition by, look at what fields will be the same for a single video.
        2. Create a second ```Window``` that will be used for ranking the videos by their trending date. By selecting the most recent date, we can retrieve the latest statistics of each video.
        3. After creating the two ```Windows```, you now can add new columns ```trendingDays``` for the number of trending days and ```rank``` for the ranking of the trending date by descending order.
        4. To get the most recent statistics, just filter the videos by their ```rank```, taking only the records with the ```rank``` 1.
        5. Drop the additional columns and cast the ```DataFrame``` to ```Dataset[VideoStats]```.

        <br>
        <details>
        <summary>Tips</summary>

        * You will need to use ```partitionBy``` and ```orderBy``` methods for the ```Window``` ; and the ```count```, ```rank``` methods from ```org.apache.spark.sql.functions``` when working with the ```Dataset```.

        </details>

        </details>
        
        To check the result of your work, go to ```App.scala```, set the ```SparkRepositories```, add the stage, and run the code. It will create the output file in the corresponding path.
        
    <br>
    <details>
    <summary>What you should know by now</summary>
  
    * How to run a ```Pipeline```.
    * Understand what is a ```Connector``` and a ```SparkRepository```, and how to set ```Deliveries``` of them.
  
    </details>

</details>

* <details>
  <summary>Achievement 3: <b>Computing the popularity score</b></summary>
  
  We are now going to compute the popularity score of each video, after getting their latest statistics. As said previously, our formula is very simple and may not represent the reality. Let's remind that the formula is ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * likesWeight + normalizedComments * commentsWeight```. Using the previous result of ```VideoStats```, we are simply going to apply the formula, and sort the data by the highest score to the lowest.
  
  1. <details>
        <summary>Configuration</summary>

        This is the last data transformation. Set the configuration so that you can save this last ```Dataset[VideoStats]```. To add the constants used for the formula, you will need to set ```Inputs``` in the ```Pipeline```. Before adding stages in the ```Pipeline```, use ```setInput[T](<value>, <id>)``` to set the constants. These inputs are retrievable anytime in any ```Factories``` once added to the ```Pipeline```.

        </details>
        
  2. <details>
        <summary>Entity</summary>

        No entity will be needed here. We will simply sort the previous data and drop the columns used for computing the score so that we can still use the ```VideoStats``` entity.

        </details>

  3. <details>
        <summary>Factory</summary>

        In this factory, all you need to do is to read the input, pass it to the ```Transformer``` that will do the data processing, and write the output. It should be pretty simple ; you can try to imitate the other ```Factories```.
        
        <details>
        <summary>Tips</summary>

        * Do not forget to set the inputs and outputs ```Deliverable```: ```Connector```, ```SparkRepository``` and/or ```Input```.

        </details>

        </details>

  4. <details>
        <summary>Transformer</summary>

        1. Let's normalize the number of likes/dislikes over the number of views. For each record, divide the number of likes by the number of views, and then the number of dislikes by the number of views. After that, get the percentage of "normalized" likes.
        2. Let's now normalize the number of comments. For each record, divide the number of comments by the number of views.
        3. We can now compute the popularity score. Remind that the formula is: ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * likesWeight + normalizedComments * commentsWeight```. <br>
         However, there are videos where comments are disabled. In this case, the formula becomes: ```views * viewsWeight + trendingDays * trendingDaysWeight + normalizedLikesPercentage * (likesWeight + commentsWeight)```. We arbitrarily decided the weights to be:
            * ```viewsWeight = 0.4```
            * ```trendingDaysWeight = 0.35```
            * ```likesWeight = 0.2```
            * ```commentsWeight = 0.05```
        
            <details><summary>Tips:</summary>
    
            * Check out ```when``` and ```otherwise``` functions from ```org.apache.spark.sql.functions```.
    
            </details>
          
        4. Sort by the ```score``` in descending order, and take the 100 first records. You now have the 100 most "popular" videos from the 10 regions.

        </details>
        
        To check the result of your work, go to ```App.scala```, set the ```Inputs``` if they are not set already, set the output ```SparkRepository```, add the stage, and run the code. It will create the output file in the corresponding path.
              
    <br>
    <details>
    <summary>What you should know by now</summary>

    * Use the three types of ```Deliveries```: ```Input```, ```Connector``` and ```SparkRepository```, with ```deliveryId```.
    * Write a ```Stage```, including the ```Factory``` and the ```Transformer(s)```.
    * Run a basic _SETL_ ETL job.
      
    </details>

</details>


# Thanks for reading ! :heart:

If you liked this project, please check out SETL Framework here: https://github.com/SETL-Developers/setl, and why not bring your contribution!
