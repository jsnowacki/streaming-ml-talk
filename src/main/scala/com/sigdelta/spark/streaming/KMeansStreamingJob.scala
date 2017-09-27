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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{DenseVector, Vector}

import scala.util.{Success, Try}

object KMeansStreamingJob {

  val appName: String = this.getClass.getSimpleName.replace("$", "")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType)))

    val points = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("checkpointLocation", "ml_checkpoint")
      .load()
      .select(from_json($"value", schema).as("json"))
      .select("json.*")
      .as[Point]

    val k = 3
    val dim = 2
    val a = 0.3

    val ds = points.map(point => point.toPointVector)

    val skm = new StructuredStreamingKMeans()
      .setK(k)
      .setDecayFactor(a)
      .setRandomCenters(dim, 0.01)
    skm.evilTrain(ds.toDF())

    val labeledPoints = ds
        .map{(pointVector: PointVector) =>
          val model = skm.getModel
          val point = Point.fromVector(pointVector.features)
          val label = model.predict(pointVector.features)
          val center = Point.fromVector(model.centers(label))
          PointCenter(point, center, label).toJson
        }

    labeledPoints.writeStream
      .format("console")
      .option("truncate", value = false)
      .start()

    val query = labeledPoints
      .writeStream
      .foreach(new SocketWriter("localhost", 9911))
      .start()

    query.awaitTermination()
  }
}
