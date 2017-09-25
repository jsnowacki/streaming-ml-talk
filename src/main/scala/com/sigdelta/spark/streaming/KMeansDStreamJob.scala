/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sigdelta.spark.streaming

import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KMeansDStreamJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.sparkContext.setLogLevel("ERROR")

    val points = ssc.socketTextStream("127.0.0.1", 9999)
      .map(Point.fromJson)
      .map(point => Vectors.dense(point.toArray))

//    points.print()
//    points.count()

    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(0.3)
      .setRandomCenters(2, 0.0)

    model.trainOn(points)
    val res = model.predictOnValues(points.map(p => (p, p)))
          .map{ case (vector: Vector, label: Int) =>
            val point = Point.fromOldVector(vector)
            val m = model.latestModel()
            val center = Point.fromOldVector(m.clusterCenters(label))
            PointCenter(point, center, label).toJson
          }

    res.foreachRDD { rdd =>
        val socket = new Socket("localhost", 9911)
        rdd.collect().foreach{ json =>
            socket.getOutputStream.write(s"$json\n".getBytes(StandardCharsets.UTF_8))
        }
    }

    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
