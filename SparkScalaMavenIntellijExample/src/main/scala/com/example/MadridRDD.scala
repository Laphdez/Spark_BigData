package com.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class Padron(codDistrito: String, descDistrito: String, codDistBarrio: String,
                  descBarrio: String, codBarrio: String, codDistSeccion: String, hombresEspanoles: Int,
                  mujeresEspanolas: Int, hombresExtranjeros: Int, mujeresExtranjeras: Int) {

  def numeroDePersonas() : Int = {
    hombresEspanoles + mujeresEspanolas + hombresExtranjeros + mujeresExtranjeras;
  }

}

object MadridRDD extends App {

  val sc = new SparkContext("local", "Simple Application", "$SPARK_HOME", null)

  val file = sc.textFile("src/main/resources/Rango_Edades_Seccion_202010.csv")

  val data = file.map(line => line.split(";").map(_.trim))
  //la cabecera del csv se excluye
  .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter).persist()

  //Map a Padron
  val dataMapped = data.map(line => (line(1), Padron(line(0), line(1),
    line(2), line(3), line(4), line(5),
    getIntValue(line(8)),
    getIntValue(line(9)),
    getIntValue(line(10)),
    getIntValue(line(11)))))
  //dataMapped.take(5).foreach(println)

  //Agrupa por distrito
  val grouppedData = dataMapped.reduceByKey((x: Padron, y: Padron) => Padron(x.codDistrito, x.descDistrito,
    x.codDistBarrio, x.descBarrio, x.codBarrio, x.codDistSeccion, x.hombresEspanoles + y.hombresEspanoles,
    x.mujeresEspanolas + y.mujeresEspanolas, x.hombresExtranjeros + y.hombresExtranjeros,
    x.mujeresExtranjeras + y.mujeresExtranjeras))

  //Ordena los hombres españoles por distrito
  grouppedData.collect().sortBy(_._2.hombresEspanoles)
    .foreach(x => println(x._2.descDistrito + " : " + x._2.hombresEspanoles))

  //sparks RDD valores estadísticos
  val spainMaleValeByDistrict = grouppedData.map(_._2.hombresEspanoles.doubleValue()).cache()
  val media = spainMaleValeByDistrict.mean()
  val stddev = spainMaleValeByDistrict.stdev()
  val max = spainMaleValeByDistrict.max()
  val min = spainMaleValeByDistrict.min()

  println("Media de Españoles varones por distrito: " + media.toInt)
  println("Desviación estandar de españoles varones por distrito: " + stddev.toInt)
  println("Num maximo de españoles varones en un distrito: " + max.toInt)
  println("Num minimo de españoles varones en un distrito: " + min.toInt)


  //ordenar la lista de personas por distritos
  grouppedData.collect().sortBy(_._2.numeroDePersonas())
    .foreach(x => println(x._2.descDistrito + " : " + x._2.numeroDePersonas))

  val numberOfPeopleByDistrict = grouppedData.map(tupla => tupla._2.numeroDePersonas().doubleValue).cache()
  val media2 = numberOfPeopleByDistrict.mean()
  val stddev2 = numberOfPeopleByDistrict.stdev()
  val max2 = numberOfPeopleByDistrict.max()
  val min2 = numberOfPeopleByDistrict.min()

  println("Media de personas por distrito: " + media2.toInt)
  println("Desviación estandar de personas por distrito: " + stddev2.toInt)
  println("Num maximo de personas en un distrito: " + max2.toInt)
  println("Num minimo de personas en un distrito: " + min2.toInt)

  def getIntValue(s:String) : Integer = {
    val value = s.substring(1, s.length - 1)
    if (value.isEmpty)
      return 0
    Integer.parseInt(value)
  }

}