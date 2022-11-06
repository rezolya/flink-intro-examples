package io.github.rezolya.intro.flink.exercises

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Try

/**
  * DataSet Job to play with IMDB ratings dataset.
  * You can get the dataset at https://ftp.fu-berlin.de/pub/misc/movies/database/frozendata -> ratings.list.gz
 */
object MoviesJob {
  val inputFile = "/tmp/flink-presentation/input/movies/ratings.list"
  val outputFolder = "/tmp/flink-presentation/output/movies"
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    // get input data
    val text = env.readTextFile(inputFile)

    //TODO: Exercise 1.1. Convert the text to objects of class Rating
    val validRatings: DataStream[Rating] = text.flatMap(Rating.parse(_))
    validRatings.writeAsText(s"$outputFolder/validRatings.txt", WriteMode.OVERWRITE).setParallelism(1)

    //TODO: Exercise 1.2. Find your favourite movie
    val favouriteMovie: DataStream[Rating] = validRatings.filter(r => r.title.startsWith("Bridget Jones"))
    favouriteMovie.writeAsText(s"$outputFolder/favouriteMovieRating.txt", WriteMode.OVERWRITE)

    //TODO: Exercise 1.3. Count words in the titles
    val wordCount: DataStream[(String, Int)] = validRatings.map(_.title.toLowerCase)
            .flatMap(t => t.split("\\W+"))
            .map(w => (w, 1))
            .keyBy(_._1)
            .sum(1)
    wordCount.writeAsText(s"$outputFolder/wordCount.txt", WriteMode.OVERWRITE)

    //TODO: Exercise 1.4. Split all ratings in 10 buckets by rank and count how many movies are in each one
    val buckets = (0 to 9).map(n => Bucket(n, n+1))
    //1. Add a bucket to the rating
    val withBuckets: DataStream[(Bucket, Rating)] = validRatings.map{r =>
          (buckets.find(b => b.isIn(r.rank)).getOrElse(Bucket(-10, -1)), r)
        }
    //2. Calculate how many ratings are in each bucket
    val bucketCount: DataStream[(Bucket, Int)] = withBuckets.map{ br => (br._1, 1) }
            .keyBy(_._1)
            .sum(1)

    bucketCount.print()

    //TODO: Exercise 1.5: Output the ranks that are not in defined buckets to a file
    val weirdRank: DataStream[Rating] = withBuckets.filter(br => br._1 == Bucket(-10, -1)).map(br => br._2)
    weirdRank.writeAsText(s"$outputFolder/weirdRank.txt", WriteMode.OVERWRITE)

    env.execute()
  }
}

case class Rating(distribution: String, votes: Long, rank: Double, title: String, year: Long, episodeDesc: String)

case class Bucket(min: Double, max: Double){
  def isIn(n: Double): Boolean = n >= min && n<max
}

object Rating {
  /**
   * Parses a line from the ratings file into a Rating object
   *
   * Example ratings file:
   * New  Distribution  Votes  Rank  Title
   *       0000000125  1888533   9.2  The Shawshank Redemption (1994)
   *       0000000125  1289428   9.2  The Godfather (1972)
   *       0000000124  889607   9.0  The Godfather: Part II (1974)
   *
   * @param input input string to parse
   * @return parsed Rating object
   */
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