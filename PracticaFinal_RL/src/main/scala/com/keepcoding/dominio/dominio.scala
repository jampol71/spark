package com.keepcoding.dominio

import java.sql.Timestamp

import org.joda.time.DateTime


case class Geolocalizacion(latitud:Double, longitud:Double, ciudad:String, pais:String)

case class Transaccion(ID_Transaccion:Long, fecha: Timestamp, producto: String, ciudad:String, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String, geolocalizacion: Geolocalizacion)

case class Cliente(ID_Transaccion: Long, nombre: String, cuentaCorriente: String)

