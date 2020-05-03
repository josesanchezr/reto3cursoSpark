import context.Context._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, count, max, min, sum, when}
import org.apache.spark.sql.types.DoubleType
import sparkSession.implicits._

object ImpuestosApp extends App {
  // Configurar el nivel del logger
  Logger.getLogger("org").setLevel(Level.WARN)

  val impuestoDf = sparkSession.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", "true")
    .load("src/main/resources/Impuesto_de_Transporte_-_MinEnerg_a.csv")

  val municipios = sparkSession.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/Departamentos_y_municipios_de_Colombia.csv")

  impuestoDf.show(5)
  municipios.show(5)

  // Registra el dataframe como una tabla temporal y envia ejecuta la sentencia sql
  val query =
    """
    select * from impuestos i inner join municipios m on i.municipio = UPPER(m.municipio)
"""
  impuestoDf.createOrReplaceTempView("impuestos")
  municipios.createOrReplaceTempView("municipios")
  sparkSession.sql(query).show(5)

  // Revisa con printSchema los nombres de la columna del dataframe de municipios
  municipios.printSchema

  // Cambia el nombre de las columnas y haz que el join contenga: {ano, trimestre,Código dane dep, departamento, Código dane municipio, municipio, total impuesto}
  val newMunicipios: DataFrame = municipios
    .withColumnRenamed("CÓDIGO DANE DEL DEPARTAMENTO", "Codigo_dane_dep")
    .withColumnRenamed("CÓDIGO DANE DEL MUNICIPIO", "Codigo_dane_municipio")

  newMunicipios.printSchema
  newMunicipios.createOrReplaceTempView("municipios")

  val query2 =
    """
    select i.ano, i.trimestre, m.Codigo_dane_dep, i.departamento,
    m.Codigo_dane_municipio, i.municipio, i.totalimpuesto
    from impuestos i inner join municipios m on i.municipio = UPPER(m.municipio)
"""
  sparkSession.sql(query2).show(5)

  // Cálcula cuantos elementos tiene el dataframe, pero que no sea una acción
  impuestoDf.select(count("*")).show()

  // Ahora cálcula la suma del totalimpuestos pagados durante el año 2019 trimestre 1
  impuestoDf
    .filter($"ano" === 2019 && $"trimestre" === 1)
    .groupBy($"ano", $"trimestre")
    .agg(sum("totalimpuesto"))
    .show()

  // Calcula el total de impuestos pagos en el año 2019 trimestre 1, el total pagado, el promedio pagadao , el menor impuesto y el mayor impuesto
  impuestoDf
    .filter($"ano" === 2019 && $"trimestre" === 1)
    .groupBy($"ano", $"trimestre")
    .agg(
      sum("totalimpuesto"),
      avg("totalimpuesto"),
      min("totalimpuesto"),
      max("totalimpuesto")
    )
    .show()

  // Agrupa la información por departamento, primero con la función de los dataframes
  impuestoDf
    .groupBy($"municipio")
    .agg(
      sum("totalimpuesto"),
      avg("totalimpuesto"),
      min("totalimpuesto"),
      max("totalimpuesto")
    )
    .show()

  val query3 =
    """
  SELECT municipio, sum(totalimpuesto), avg(totalimpuesto), min(totalimpuesto), max(totalimpuesto) FROM impuestos GROUP BY municipio
  """
  sparkSession.sql(query3).show(5)

  // Calcula el totalimpuesto cancelado por cada departamento ordenado desc
  val query4 =
    """
  SELECT departamento, sum(totalimpuesto) as totalimppagado FROM impuestos GROUP BY departamento ORDER BY totalimppagado DESC
  """
  sparkSession.sql(query4).show(5)

  // Ejercicio Final

  /*
  Instruccciones:

  1. Definir la sesión de Spark.
    R/. Ya esta creada como sparkSession
  2. Crear un dataframe desde un archivo plano ubicación: resources/Impuesto_de_Transporte_-MinEnerga.csv y registra como una tabla temporal
    R/. Ya esta creado como impuestoDf
  3. Obten el total impuesto que pago cada departamento anualmente con dos aproach diferentes:
      a) Utiliza el aproach de groupBy
      b) Utiliza el aproach de window functions over partition
  4. Calcula el acumulado que ha pagado cada departamento anualmente. Lo pagado el año actual y el acumulado que se ha cancelado año tras año
  5. Ahora quisieramos un analisis sobre la tendencia del impuestototal de manera anual, realiza los siguientes pasos:
      a) Calcula el impuestototal que pago cada departamento y en la siguiente columna el impuesto que cancelo el año anterior.
      b) Con estos valores calculados, agrega la diferencia del impuesto cancelado y en otra columna si fue {igual,bajó, subió}
        Puedes utilizar esta función def when(condition: Column, value: Any)
   */

  // Resolviendo punto 3.a:
  impuestoDf
    .groupBy("ano", "departamento")
    .agg(sum("totalimpuesto").alias("totalano"))
    .orderBy("departamento")
    .show(5)

  // Resolviendo punto 3.b:
  val windowSpec = Window
    .partitionBy("ano", "departamento")
    .orderBy("departamento")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  impuestoDf
    .select(
      $"ano",
      $"departamento",
      $"totalimpuesto".as("totalano"),
      sum("totalimpuesto").over(windowSpec).alias("totalacc")
    )
    .distinct()
    .show(10)

  // result calcula el impuesto por departamento por año
  // Resolviendo punto 4:
  val result = impuestoDf
    .groupBy("ano", "departamento")
    .agg(sum("totalimpuesto") as "totalimpuesto")
    .orderBy("departamento")
  val windowSpec2 = Window
    .partitionBy("departamento")
    .orderBy("ano")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  result
    .select(
      $"ano",
      $"departamento",
      $"totalimpuesto".as("totalano"),
      sum("totalimpuesto").over(windowSpec2).alias("totalacc")
    )
    .show(10)

  // Resolviendo punto 5a:
  val windowSpec3 = Window
    .partitionBy("departamento")
    .orderBy("ano")
    .rowsBetween(Window.currentRow - 1L, Window.currentRow - 1L)
  val result2 = result
    .select(
      $"ano",
      $"departamento",
      $"totalimpuesto".as("totalano"),
      sum("totalimpuesto").over(windowSpec3).alias("lastyear")
    )
    .na
    .fill(0, Array("lastyear"))
  result2.show(10)

  val result3 = result2
    .select(
      $"ano",
      $"departamento",
      $"totalano",
      $"lastyear",
      ($"totalano" - $"lastyear")
        .cast(DoubleType)
        .alias("difference")
    )
  result2.show(10)

  result3.printSchema()

  result3
    .select(
      $"ano",
      $"departamento",
      $"totalano",
      $"lastyear",
      $"difference",
      when($"difference" === 0, "Igual")
        .when($"difference" < 0, "Bajó")
        .otherwise("Subió")
        .alias("Trend")
    )
    .show(10)

  // Finalizar sesión de Spark
  sparkSession.stop()
}
