package io.github.rezolya.intro.flink.exercises

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

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
    val text = env.readTextFile("/tmp/movies/ratings.list")

    //TODO: Exercise 1.1. Convert the text to objects of class Rating
    val validRatings: DataSet[Rating] = ???
    validRatings.writeAsText("/tmp/movies/validRatings.txt", WriteMode.OVERWRITE)

/*
    //TODO: Exercise 1.2. Find your favourite movie
    val favouriteMovie: DataSet[Rating] = ???
    favouriteMovie.writeAsText("/tmp/movies/favouriteMovieRating.txt", WriteMode.OVERWRITE)

    //TODO: Exercise 1.3. Count words in the titles
    val wordCount: DataSet[(String, Int)] = ???
    wordCount.writeAsText("/tmp/movies/wordCount.txt", WriteMode.OVERWRITE)
*/

    env.execute()
  }
}

case class Rating(distribution: String, votes: Long, rank: Double, title: String, year: Long, episodeDesc: String)

object Rating {
  def parse(input: String): Option[Rating] = {
    if (input.startsWith("      ")) {
      Try {
        val split = input.trim.split("\\s+").toList
        val episodePosition = split.indexWhere(s => s.startsWith("{"))
        val yearPosition = if(episodePosition == -1) split.length-1 else episodePosition-1
        val year = split(yearPosition).replaceAll("[()/I]", "").toLong
        val title = split.slice(3, yearPosition).mkString(" ")
        val episodeDesc = if(episodePosition == -1) "" else split.slice(episodePosition, split.length-1).mkString(" ")
        Rating(split(0), split(1).toLong, split(2).toDouble, title, year, "")
      }.toOption
    }
    else None
  }
}