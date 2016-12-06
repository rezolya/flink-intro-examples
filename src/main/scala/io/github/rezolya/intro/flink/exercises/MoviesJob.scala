package io.github.rezolya.intro.flink.exercises

import org.apache.flink.api.scala._

import scala.util.Try

/**
  * DataSet Job to play with IMDB ratings dataset.
  * You can get the dataset at ftp://ftp.fu-berlin.de/pub/misc/movies/database/ratings.list.gz
 */
object MoviesJob {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.readTextFile("/tmp/ratings.list")

    //TODO: Exercise 1.1. Convert the text to objects of class Rating
    val validRatings: DataSet[Rating] = ???
    validRatings.writeAsText("/tmp/movies/validRatings.txt")

    //TODO: Exercise 1.2. Find your favourite movie
    val favouriteMovie: DataSet[Rating] = ???
    favouriteMovie.writeAsText("/tmp/favouriteMovieRating.txt")

    //TODO: Exercise 1.3. Count words in the titles
    val wordCount: DataSet[(String, Int)] = ???
    wordCount.writeAsText("/tmp/movies/wordCount.txt")

    //TODO: Exercise 1.4. Split all ratings in 10 buckets by rank and count how many movies are in each one
    val buckets = (0 to 9).map(n => Bucket(n, n+1))
    //1. Add a bucket to the rating
    val withBuckets: DataSet[(Bucket, Rating)] = ???
    //2. Calculate how many ratings are in each bucket
    val bucketCount: DataSet[(Bucket, Int)] = ???

    bucketCount.print()

    env.execute()
  }
}

case class Rating(distribution: String, votes: Long, rank: Double, title: String, year: Long, episodeDesc: String)

case class Bucket(min: Double, max: Double){
  def isIn(n: Double) = n >= min && n<max
}

object Rating {
  def parse(input: String): Option[Rating] = {
    if (input.startsWith("      ")) {
      Try {
        val split = input.trim.split("\\s+").toList
        val yearPosition = split.indexWhere(s => s.startsWith("("))
        val year = split(yearPosition).replaceAll("[()/I]", "").toLong
        val title = split.slice(3, yearPosition).mkString(" ")
        Rating(split(0), split(1).toLong, split(2).toDouble, title, year, "")
      }.toOption
    }
    else None
  }
}