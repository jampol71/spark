package com.keepcoding.speedlayer

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MetricasSparkStreaming {

  def run(args: Array[String]): Unit = {

    //Definicion de la configuracion de Spark

    val conf = new SparkConf().setMaster("local[*]").setAppName("Practica Final RL - Streaming")


    //Definicion del nombre del topico de kafka

    val inputTopic = "prueba"

    //Definicion de la configuracion de Kafka, servidor, deserializador, serializador

    val kafkaParams = Map[String, Object](
      elems = "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    //Definicion del contexto de spark streaming

    val ssc = new StreamingContext(conf, Seconds(8))


    //Definicion del DStream

    val inputStream  = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array(inputTopic), kafkaParams))


    //Coge del DStream la parte de value

    val transaccionesStream: DStream[String] = inputStream.map(_.value())



    //Funcion para parsear la fecha
    def Str2Date(cadena: String): Timestamp = {
      val cadena_limpia = cadena.trim()
      val fecha = cadena_limpia //.substring(1)
      val pattern = "MM/dd/yy HH:mm"

      return new Timestamp(new SimpleDateFormat("MM/dd/yy HH:mm").parse(fecha).getTime())

    }


    def writeToKafka(outputTopic: String)(partitionOfRecords: Iterator[Row]): Unit = {
      val producer = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data =>
        producer.send(new ProducerRecord[String, String](outputTopic, data.toString)))
      producer.flush()
      producer.close()
    }


    def getKafkaConfig(): Properties = {
      val prop = new Properties()
      prop.put("bootstrap.servers", "localhost:9092")
      prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      prop
    }




    //Transforma el DStream en 2 objetos:Cliente y Transaccion. Esto lo almacena en una tupla

    val streamTransformado = transaccionesStream.map(_.toString().split(",")).map(columna => {
      new Tuple2(Cliente(scala.util.Random.nextInt(), columna(4).toString, columna(6).toString), Transaccion(scala.util.Random.nextInt(), Str2Date(columna(0)), columna(1).toString, columna(5).toString, columna(2).toDouble, columna(10).toString, "N/A", columna(3).toString, Geolocalizacion(columna(8).toDouble, columna(9).toDouble, columna(5).toString, "N/A")))

    })


    //Pasar de dstream a DF para poder trabajar con SQL declarativo


    streamTransformado.foreachRDD(foreachFunc = rdd => {

      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val clientesDF = rdd.map(t => t._1).toDF()


      clientesDF.createOrReplaceGlobalTempView(viewName = "Clientes")



      val TransaccionesDF = rdd.map(t => t._2).toDF()

      TransaccionesDF.createOrReplaceGlobalTempView(viewName = "Transacciones")

      TransaccionesDF.show()



        //Tarea2

      println("Tarea2: Agrupa todos los clientes por ciudad. Contar todas las transacciones por ciudad. El objetivo sería contar todas las transacciones ocurridas por ciudad*****************************************************************************************")
      val dfTransaccionesCiudad = spark.sql(sqlText = "SELECT ciudad, COUNT(*) as numTransacciones FROM global_temp.Transacciones GROUP BY ciudad order by numTransacciones DESC")

      dfTransaccionesCiudad.show()

      dfTransaccionesCiudad.rdd.foreachPartition(writeToKafka("t2"))

      //Tarea3

      println("Tarea3: Encuentra todos los clientes que hayan realizado pagos > 2500*******************************************************************************************************")

      val dfTransaccionesImporte = spark.sql(sqlText = "SELECT t.fecha, t.ciudad, t.importe, t.descripcion FROM global_temp.Transacciones t WHERE importe > '2500' SORT BY importe")

      dfTransaccionesImporte.show()
      dfTransaccionesImporte.rdd.foreachPartition(writeToKafka("t3"))


    ///Tarea4. He intenteado realizar un join pero el campo común de la parte batch, ahora es un número aleatorio que no es común entre las 2 tablas

      println("Tarea4: Obten todas las transacciones cuya ciudad sea London********************************************************************************************")

      val dfTransaccionesLondon = spark.sql(sqlText = "SELECT t.fecha, t.importe, t.descripcion FROM global_temp.Transacciones t WHERE ciudad == 'London' SORT BY importe")

      dfTransaccionesLondon.show()

      dfTransaccionesLondon.rdd.foreachPartition(writeToKafka("t4"))


      //tarea5

      println("Tarea5: Mostrar todas las transacciones que pertenezcan a ocio****************************************************************************************************************")

      val dfTransaccionesOcio = spark.sql(sqlText = "SELECT * FROM global_temp.Transacciones WHERE ( (TRIM(descripcion)=='Cinema') OR (TRIM(descripcion)=='Sports'))")

      dfTransaccionesOcio.show()

      dfTransaccionesOcio.rdd.foreachPartition(writeToKafka("t5"))


      //Tarea6

      println("Tarea6: Transacciones realizadas a partir del 20 enero********************************************************************************************")
      val dfUltimasTransacciones = spark.sql(sqlText= "SELECT * FROM global_temp.Transacciones t WHERE fecha between '2009-01-20' and '2009-02-01' sort by t.fecha")

      dfUltimasTransacciones.show()

      dfUltimasTransacciones.rdd.foreachPartition(writeToKafka("t6"))

      //Tarea7 (Extra1)
      println("Tarea7: Ciudades donde el importe de la transaccion es mayor********************************************************************************************")
      val dfMayorIimporte = spark.sql(sqlText= "SELECT t.ciudad FROM global_temp.Transacciones t  sort by t.importe")

      dfMayorIimporte.show()

      dfMayorIimporte.rdd.foreachPartition(writeToKafka("t7"))

      //Tarea8 (Extra2)
      println("Tarea8: Transacciones realizadas el fin de semana********************************************************************************************")
      val dfFinSemana = spark.sql(sqlText= "SELECT ID_Transaccion FROM global_temp.Transacciones t  WHERE ((fecha == '2009-01-03') OR (fecha == '2009-01-04') OR (fecha == '2009-01-10') OR (fecha == '2009-01-11') OR (fecha == '2009-01-17') OR (fecha == '2009-01-18') OR (fecha == '2009-01-24') OR (fecha == '2009-01-25')) sort by fecha")

      dfFinSemana.show()

      dfFinSemana.rdd.foreachPartition(writeToKafka("t8"))

    } )



    ssc.start()
    ssc.awaitTermination()

  }
}

