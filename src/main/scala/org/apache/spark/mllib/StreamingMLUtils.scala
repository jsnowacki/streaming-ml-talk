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

// from https://github.com/holdenk/spark-structured-streaming-ml

package org.apache.spark.mllib

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.util.MLUtils

object StreamingMLUtils {
  implicit def mlToMllibVector(v: Vector): OldVector = v match {
    case dv: DenseVector => OldVectors.dense(dv.toArray)
    case sv: SparseVector => OldVectors.sparse(sv.size, sv.indices, sv.values)
    case _ => throw new IllegalArgumentException
  }

  def fastSquaredDistance(x: Vector, xNorm: Double, y: Vector, yNorm: Double): Double = {
    MLUtils.fastSquaredDistance(x, xNorm, y, yNorm)
  }
}
