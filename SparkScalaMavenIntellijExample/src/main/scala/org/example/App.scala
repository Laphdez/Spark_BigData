package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author
 */
object App {

  def main(args : Array[String]) {

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddCollect:Array[Int] = rdd.collect()
    println("Number of Partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)

/*
    val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = spark.sparkContext.parallelize(dataSeq)
    println("Numero de Particiones: " + rdd.getNumPartitions)
    println("Primer elemento: " + rdd.first())
    println("Numero de elementos) " + rdd.count())
    println("RDD converted to Array[Int] : ")
    rdd.foreach(println)

*/
  }

}
