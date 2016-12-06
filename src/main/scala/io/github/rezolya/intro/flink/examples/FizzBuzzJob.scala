package io.github.rezolya.intro.flink.examples

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SerializationSchema

/**
 * FizzBuzz streaming example.
  * Open sockets before running this job:
  *  nc -l 9998             - input socket
  *  nc -l 127.0.0.1 9999   - here the output will show
 */
object FizzBuzzJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.socketTextStream("localhost", 9998)

    val numbers: DataStream[Int] = env.fromElements(1, 2, 3)
    val times3: DataStream[Int] = numbers.map(n => n*3)               //3, 6, 9
    val repeated: DataStream[Int] = times3.flatMap(n => Seq(n, n))    //3, 3, 6, 6, 9, 9
    val even: DataStream[Int] = repeated.filter(n => n%2==0)          //6, 6
    //val numbers = text.map(t => t.toInt)

    val fizzbuzz = numbers.map{ n =>
      (n % 3, n % 5) match {
        case (0, 0) => "FizzBuzz"
        case (0, _) => "Fizz"
        case (_, 0) => "Buzz"
        case _ => n.toString
      }
    }

    val serialisationSchema = new SerializationSchema[String]() {
      override def serialize(t: String): Array[Byte] = s"$t\n".getBytes
    }

    fizzbuzz.addSink(new SocketClientSink("localhost", 9999, serialisationSchema, 0, true))

    even.writeAsText("/tmp/numbers.txt", WriteMode.OVERWRITE)
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}