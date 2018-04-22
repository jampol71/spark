package com.keepcoding.batchlayer

import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import java.sql.Timestamp
import java.text.SimpleDateFormat

object MetricasSparkSQL {

  def run (args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master(master = "local[*]")
      .appName(name = "Practica Final - Batch Layer - Spark SQL - RL")
      .getOrCreate()


    //Para poder realizar conversiones de RDD a DF
    import  spark.implicits._

    //Para evitar que salga texto en el log
    spark.sparkContext.setLogLevel("ERROR")

     //Es un DF
    val rddTransacciones = spark.read.csv(s"file:///${args(0)}")

    val cabecera = rddTransacciones.first

    val rddSinCabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString().split(","))








    /*
    case class Cliente(ID_Transaccion: Long, nombre: String, cuentaCorriente: String)
    */

    //Mapear DF mediante la clase

    val acumulador: LongAccumulator = spark.sparkContext.longAccumulator(name = "IDCliente")
    val dfClientes = rddSinCabecera.map ( columna => {
      acumulador.add(1)
      Cliente(acumulador.value, columna(4), columna(6))

    })

    dfClientes.show(numRows = 10)


    //Funcion para parsear la fecha

    def Str2Date(cadena: String): Timestamp = {
      val cadena_limpia = cadena.trim()
      val fecha = cadena_limpia.substring(1)
      val pattern = "MM/dd/yy HH:mm"

      return new Timestamp(new SimpleDateFormat(pattern).parse(fecha).getTime())


    }


/*
    case class Transaccion(ID_Transaccion:Long, fecha:String, producto: String, ciudad:String, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String, geolocalizacion: Geolocalizacion)
*/
    val acumuladorTransaccion: LongAccumulator = spark.sparkContext.longAccumulator(name = "IDTransaccion")
    val dfTransacciones = rddSinCabecera.map(columna => {
      acumuladorTransaccion.add(1)
      Transaccion(acumuladorTransaccion.value,Str2Date(columna(0)), columna(1), columna(5).trim, columna(2).toDouble, columna(10).stripSuffix("]"), "N/A", columna(3), Geolocalizacion(columna(8).toDouble, columna(9).toDouble, columna(5), "N/A"))

    })

    dfTransacciones.show(numRows = 10)


    //Creamos las tablas temporales para hacer las queries
    dfTransacciones.createOrReplaceGlobalTempView(viewName = "TRANSACCIONES")

    dfClientes.createOrReplaceGlobalTempView(viewName = "CLIENTES")


    //join de los df transaciones y clientes


    println("Tabla con el join de transacciones y clientes***************************************************************************************************************************")
    val dfCompleta = spark.sql("SELECT t.fecha, t.ciudad, t.descripcion, c.nombre FROM global_temp.TRANSACCIONES as t JOIN global_temp.CLIENTES as c ON t.ID_Transaccion = c.ID_Transaccion ")



    dfCompleta.createOrReplaceGlobalTempView(viewName = "TABLA")


    dfCompleta.show(10)


    println("Tarea2: Agrupa todos los clientes por ciudad. Contar todas las transacciones por ciudad. El objetivo sería contar todas las transacciones ocurridas por ciudad*****************************************************************************************")


    val dfTransaccionesCiudad = spark.sql(sqlText = "SELECT t.ciudad, COUNT(*) as transaccionesRealizadas FROM global_temp.TABLA as t GROUP BY t.ciudad ORDER BY transaccionesRealizadas DESC")

    dfTransaccionesCiudad.show()





    //Escribimos en el fichero en formato csv
    dfTransaccionesCiudad.write.format(source = "csv").mode(saveMode = "overwrite").save(path = args(1)+"/T2")


    //Encuentra todos los clientes que hayan realizado pagos > 5000
    println("Tarea3: Encuentra todos los clientes que hayan realizado pagos > 5000*******************************************************************************************************")

    val dfTransaccionesImporte =   spark.sql(sqlText = "SELECT transacciones.fecha, transacciones.ciudad, transacciones.importe, transacciones.descripcion FROM global_temp.TRANSACCIONES transacciones WHERE importe > 5000 SORT BY importe DESC")
    dfTransaccionesImporte.show()

    dfTransaccionesImporte.write.format(source = "csv").mode(saveMode = "overwrite").save(path = args(1)+"/T3")


    println("Tarea4: Obten todas las transacciones agrupadas por cliente cuya ciudad sea London********************************************************************************************")

    val dfTransaccionesLondon = spark.sql(sqlText = "SELECT t.nombre FROM global_temp.TABLA as t WHERE t.ciudad =='London' GROUP BY t.nombre ")
    dfTransaccionesLondon.show()

    dfTransaccionesLondon.write.format(source = "csv").mode(saveMode = "overwrite").save(path = args(1)+"/T4")


    //Mostrar todas las operaciones cuya categoria sea ocio

    println("Tarea5: Mostrar todas las transacciones que pertenezcan a ocio****************************************************************************************************************")

    val dfTransaccionesOcio =  spark.sql(sqlText = "SELECT * FROM global_temp.TABLA as t WHERE (TRIM(t.descripcion)=='Cinema' OR (TRIM(t.descripcion)=='Sports'))")
    dfTransaccionesOcio.show()

    dfTransaccionesOcio.write.format(source = "csv").mode(saveMode = "overwrite").save(path = args(1)+"/T5")

    println("Tarea6: Obten las ultimas transacciones de cada cliente en los ultimos 30 dias****************************************************************************************************************")




    val dfUltimasTransacciones = spark.sql(sqlText= "SELECT * FROM global_temp.TABLA as t WHERE fecha between '2009-01-01' and '2009-02-01' sort by t.fecha").show()


  }

}

