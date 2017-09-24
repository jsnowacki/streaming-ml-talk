package com.sigdelta.spark.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{DenseVector, Vector}

case class Point(x: Double, y: Double) {
  def toPointVector: PointVector = {
    val v = new DenseVector(Seq(this.x, this.y).toArray)
    PointVector(v)
  }
}
case class PointVector(features: Vector)

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

//    points
//      .writeStream
//      .format("console")
//      .option("truncate", value = false)
//      .start()

    val k = 3
    val dim = 2
    val clusterSpread = 0.1
    val seed = 63

    val ds = points.map(point => point.toPointVector)

//    ds
//      .writeStream
//      .format("console")
//      .option("truncate", value = false)
//      .start()
//      .awaitTermination()

    val skm = new StreamingKMeans().setK(k).setRandomCenters(dim, 0.01)
    skm.evilTrain(ds.toDF())
    val model = skm.getModel

    val labeledPoints = model.transform(ds)

    val query = labeledPoints.writeStream
      .format("console")
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
  }
}
