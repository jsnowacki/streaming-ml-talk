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
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue[Point](line)
  }

  def fromOldVector(vector: OldVector): Point = {
    val a = vector.toArray
    Point(a(0), a(1))
  }

  def fromVector(vector: Vector): Point = {
    val a = vector.toArray
    Point(a(0), a(1))
  }
}

case class PointVector(features: Vector)

case class PointCenter(point: Point, center: Point, label: Int) {
  def toJson: String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    mapper.writeValueAsString(this)
  }
}

