package com.keepcoding

import com.keepcoding.speedlayer.MetricasSparkStreaming
import org.apache.log4j.{Level, Logger}


object StreamingApplication {


  Logger.getLogger("org").setLevel(Level.FATAL)
  def main(args: Array[String]): Unit = {

    if (args.length == 2){
      MetricasSparkStreaming.run(args)
    }else{
      println("Se está intentando arrancar el job de spark sin los parámetros necesarios")
    }

  }

}
