package org.apache.spark

import org.apache.spark.ml.linalg.{BLAS => SparkBLAS, _}

object BLAS {
  def axpy(a: Double, x: Vector, y: Vector): Unit = SparkBLAS.axpy(a, x, y)

  def scal(a: Double, x: Vector): Unit = SparkBLAS.scal(a, x)

  def dot(x: Vector, y: Vector): Double = SparkBLAS.dot(x, y)
}
