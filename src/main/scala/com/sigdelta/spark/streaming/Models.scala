package com.sigdelta.spark.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.{Vector => OldVector}

case class Point(x: Double, y: Double) {

  def toArray: Array[Double] = Seq(this.x, this.y).toArray

  def toPointVector: PointVector = {
    val v = new DenseVector(toArray)
    PointVector(v)
  }
}

object Point {
  def fromJson(line: String): Point = {
    val mapper =  new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue[Point](line)
  }

  def fromOldVector(vector: OldVector): Point = {
    val a = vector.toArray
    Point(a(0), a(1))
  }
}

case class PointVector(features: Vector)

case class PointCenter(point: Point, center: Point, label: Int) {
  def toJson: String = {
    val mapper =  new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.writeValueAsString(this)
  }
}

