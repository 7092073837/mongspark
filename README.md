# Spark_RDD
Movie Lensdata with Spark Scala
In this assignment, we will use Spark to have a look at the Movie Lens dataset containing user generated ratings for movies. The dataset comes in 3 files:

 1.movies.csv contains meta information about the movies: movieId,title,genres
 2.ratings.csv contains the ratings in the following format: userId,movieId,rating,timestamp
 3.tags.csv contains users with tags information about the tags: userId,movieId,tag,timestamp
 
 Refer to the README for the detailed description of the data.

Requirement:


https://alex-mailajalam@bitbucket.org/alex-mailajalam/meritgroup.git

Environment
    Linux (Ubuntu 18.04)
    Hadoop 2.7.2
    Spark 2.0.2
    Scala 2.11.7


Installation steps

    1.Clone the repository

    2.git clone https://alex-mailajalam@bitbucket.org/alex-mailajalam/meritgroup.git
      cd meritGroup/

    3.build sbt Using InteljIDEA 
    
    4.If running from spark-shell then change your working directory to org.com.analytics.df or org.com.analytics.rdd locations

    5.Run the Movie_Driver Object to view results
    
Analytics Assignments:

Spark RDD & SPARK DATAFRAME

1. How many “Drama” movies (movies with the "Drama" genre) are there?
2. How many unique movies are rated, how many are not rated?
3. Who give the most ratings, how many rates did he make?
4. Compute min, average, max rating per movie.
5. Output dataset containing users that have rated a movie but not tagged it.
6. Output dataset containing users that have rated AND tagged a movie.
7. Describe how you would find the release year for a movie (refer to the readme for information).
8. Enrich movies dataset with extract the release year. Output the enriched dataset.
9. Output dataset showing the number of movies per Genre per Year (movies will be counted many times if it's associated with multiple genres).

Summary of steps to do:

1. Import the project to any IDE(preferably Intellij)
2. Change the input files location in org.com.analytics.df.loadSourcedata.scala
3. org.com.analytics.df.MoviesAnalytics.scala is the driver program which will invoke all the other classes respective for the use cases.
