# Youtube SETL

Youtube SETL is a project that aims at providing a starting point to practice the SETL Framework : https://github.com/JCDecaux/setl. The idea is to give a context project involving Extract, Transform and Load operations. There are three levels of difficulty for the exercise : Easy, Medium and Difficult.

The data that is used is from Kaggle, https://www.kaggle.com/datasnaek/youtube-new.

# Introduction

The goal of this project is to find the 100 most "popular" videos, the most "popular" categories of videos as well as the 100 most "popular" videos per category. Below are the instructions for each difficulty level to realize the project. For each difficulty level, you can clone the repo with the specific branch to have a starting project.

# Context

The data is divided in multiples regions: Canada (CA), Germany (DE), France (FR), Great Britain (GB), India (IN), Japan (JP), South Korea (KR), Mexico (MX), Russia (RU) and the United States (US). For each of these regions, there are two files:
1. A CSV file, containing the following columns: <details><summary></summary>![](CSV_fields.png? "CSV File Description")</details>

2. A JSON file, containing three keys:
    1. kind: String
    2. etag: String
    3. items: Array of objects

Basically, the elements of the *items* fields allow us to map the ```category_id``` of the CSV file to the full name category.

We are going to analyze this dataset and determine "popular" videos. But, how do we define a popular video ? We are going to define the popularity of a video based on its publish time, number of views, likes, dislikes and comments.

This definition is clearly debatable and arbitrary, and we are not looking to find out the best definition for the popularity of a video. We will only focus on the purpose of this project: practice with the SETL Framework.

## Difficult

### Instructions

* <details>
  <summary>Achievement 1</summary>
  
  * You are on your own ! Do whatever you please in order to achieve the tasks.

</details>


## Medium

### Instructions

* <details>
  <summary>Achievement 1</summary>
  
  *

</details>

## Easy

### Instructions

* <details>
  <summary>Achievement 1</summary>

  *

</details>


### Thanks for reading ! :heart:

If you liked this project, please check out SETL Framework here : https://github.com/JCDecaux/setl, and why not bring your contribution !
