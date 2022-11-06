package io.github.rezolya.intro.flink.exercises
//import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
//import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
//import org.apache.flink.api.scala._
//import org.apache.flink.core.fs.FileSystem.WriteMode
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * DataStream Job to play with TaxiRide data stream, provided by data artisans for training purposes
  * You can get the dataset at: http://dataartisans.github.io/flink-training/trainingData/nycTaxiRides.gz
  */
object TaxisJob {
//  def main(args: Array[String]) {
//
//    // set up the execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // configure event-time processing
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //env.setParallelism(1)
//
//    val maxDelay = 1
//    val servingSpeed = 60
//
//    // get the taxi ride data stream
//    val rides: DataStream[TaxiRide] = env.addSource(
//      new TaxiRideSource("/tmp/nycTaxiRides.gz", maxDelay, servingSpeed))
//
//    //TODO: Exercise 2.1. Write the taxi rides to a socket stream, and see how this works
//
//    //TODO: Exercise 2.2. Taxi Ride Cleansing. Filter out only taxi rides which start and end in the New York City
//    //GeoUtils.isInNYC() can tell you whether a location is in NYC.
//    val nyRides: DataStream[TaxiRide] = ???
//    nyRides.writeAsText("/tmp/taxis/nyRides.txt", WriteMode.OVERWRITE)
//
//    /* TODO: Exercise 3. Popular places. Find popular places by counting places where the taxi rides stop or start
//       use GeoUtils.mapToGridCell(float lon, float lat) to group locations
//       count every five minutes the number of taxi rides that started and ended in the same area within the last 15 minutes
//    */
//
//    //1. determine the sells of start and end of the taxi rides
//    // result type: (Int - cell id, TaxiRide)
//    val withCell: DataStream[(Int, TaxiRide)] = ???
//
//    //2. find out how many times each cell is visited
//    // result type: (Int - cell id, Long - timestamp of the count, Boolean - arrival or departure, Int - count)
//    val cellVisits: DataStream[(Int, Long, Boolean, Int)] = ???
//
//    //3. filter the popular cells only - the ones that have more visits that popularityThreshold
//    val popularityThreshold = 20
//    val popularCells: DataStream[(Int, Long, Boolean, Int)] = ???
//
//    //4. convert the cell id to coordinates of the cell (use GeoUtils.getGridCellCenterLat(cellId))
//    // result type: (Float - cell longitude, Float - cell latitude, Long - timestamp of the count, Boolean - arrival or departure, Int - count)
//    val popularPlaces: DataStream[(Float, Float, Long, Boolean, Integer)] = ???
//    popularPlaces.writeAsText("/tmp/taxis/popularPlaces.txt", WriteMode.OVERWRITE)
//
//    env.execute()
//  }

}
