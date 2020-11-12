package com.example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MadridDF {

  def main(args : Array[String]) {

    val sparkSession = SparkSession.builder().master("local")
      .appName("Spark Application")
      .getOrCreate()
    //custom scheme definition
    val customSchema = StructType(Array(
      StructField("COD_DISTRITO", StringType, nullable = true),
      StructField("DESC_DISTRITO", StringType, nullable = true),
      StructField("COD_DIST_BARRIO", StringType, nullable = true),
      StructField("DESC_BARRIO", StringType, nullable = true),
      StructField("COD_BARRIO", StringType, nullable = true),
      StructField("COD_DIST_SECCION", StringType, nullable = true),
      StructField("COD_SECCION", StringType, nullable = true),
      StructField("COD_EDAD_INT", StringType, nullable = true),
      StructField("EspanolesHombres", IntegerType, nullable = true),
      StructField("EspanolesMujeres", IntegerType, nullable = true),
      StructField("ExtranjerosHombres", IntegerType, nullable = true),
      StructField("ExtranjerosMujeres", IntegerType, nullable = true)))

    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter" , ";")
      .option("nullValue", null)
      .schema(customSchema)
      .load("src/main/resources/Rango_Edades_Seccion_202010.csv")

    df.show(true)

    //fill empty values with 0
    val dfNA = df.na.fill(0, Seq("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres")).cache()
    dfNA.show(true)

    dfNA.groupBy("DESC_DISTRITO")
      .agg(avg("EspanolesHombres"), avg("EspanolesMujeres"), avg("ExtranjerosHombres"), avg("ExtranjerosMujeres"))
      .show()

    dfNA.groupBy("DESC_DISTRITO")
      .agg(max("EspanolesHombres"), stddev("EspanolesHombres"), max("EspanolesMujeres"), stddev("EspanolesMujeres"))
      .show()

    dfNA.groupBy("DESC_DISTRITO")
      .agg(max("ExtranjerosHombres"), stddev("ExtranjerosHombres"), max("ExtranjerosMujeres"), stddev("ExtranjerosMujeres"))
      .show()

    /*
    dfNA.groupBy("DESC_DISTRITO")
      .agg(min("EspanolesHombres"), stddev("EspanolesHombres"), min("EspanolesMujeres"), stddev("EspanolesMujeres"))
      .show()
    */
    val dfNASum = dfNA.groupBy(df("DESC_DISTRITO"))
      .sum("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres").cache()

    //Personas totales por distrito
    dfNASum.select(dfNASum("DESC_DISTRITO"), (dfNASum("sum(EspanolesHombres)") + dfNASum("sum(EspanolesMujeres)") + dfNASum("sum(ExtranjerosHombres)") + dfNASum("sum(ExtranjerosMujeres)")).alias("total"))
      .sort(desc("total"))
      .show(40)

  }
}

