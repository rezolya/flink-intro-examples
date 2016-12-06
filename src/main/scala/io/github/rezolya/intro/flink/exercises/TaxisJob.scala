package io.github.rezolya.intro.flink.exercises
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * DataStream Job to play with TaxiRide data stream, provided by data artisans for training purposes
  * You can get the dataset at: http://dataartisans.github.io/flink-training/trainingData/nycTaxiRides.gz
  */
object TaxisJob {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // configure event-time processing
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val maxDelay = 1
    val servingSpeed = 60

    // get the taxi ride data stream
    val rides: DataStream[TaxiRide] = env.addSource(
      new TaxiRideSource("/tmp/nycTaxiRides.gz", maxDelay, servingSpeed))

    //TODO: Exercise 2.1. Write the taxi rides to a socket stream, and see how this works

    //TODO: Exercise 2.2. Taxi Ride Cleansing. Filter out only taxi rides which start and end in the New York City
    //GeoUtils.isInNYC() can tell you whether a location is in NYC.
    val nyRides: DataStream[TaxiRide] = ???
    nyRides.print()

    //TODO: Exercise 2.3. Popular places. Find popular places by counting places where the taxi rides stop or start
    // output is a Tuple[cell longitude, cell latitude, timestamp of the count, arrival or departure, count]
    val popularPlaces: DataStream[(Float, Float, Long, Boolean, Integer)] = ???

    env.execute()
  }

}
