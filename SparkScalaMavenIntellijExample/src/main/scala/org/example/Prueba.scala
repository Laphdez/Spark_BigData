package org.example

import org.apache.spark.{SparkConf, SparkContext}

object Prueba {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("prueba"))
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x)
    println(result.collect().mkString("-"))
    result.first()
  }
}
