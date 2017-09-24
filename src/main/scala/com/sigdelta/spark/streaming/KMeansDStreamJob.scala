package com.sigdelta.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KMeansDStreamJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.sparkContext.setLogLevel("ERROR")

    val points = ssc.socketTextStream("127.0.0.1", 9999)
      .map(Point.fromJson)
      .map(point => Vectors.dense(point.toArray))

    points.print()
    points.count()

    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(points)
    val res = model.predictOnValues(points.map(p => (p, p)))
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
