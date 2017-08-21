package kafka.vispro

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import scala.tools.nsc.doc.model.Val

case class FlightKey(airLineId: String, arrivalAirPortId: Int, arrivalDelay: Double)
case class DelayedFlight(airline: String, head: Int, origAirport: String, originCity: String, destAirport: String, destCity: String, arrivalDelay: Int)

object SecondarySorting {

  def createKeyValueTuple(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), listData(data))
  }

  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(0), data(1).toInt, data(2).toDouble)
  }

  def listData(data: Array[String]): List[String] = {
    List(data(0), data(1), data(2), data(3))
  }
  
  /*  def createDelayedFlight(key: FlightKey, data: List[String], bcTable: Broadcast[RefTable]): DelayedFlight = {
    val table = bcTable.value
    val airline = table.get(AIRLINE_DATA, key.airLineId)
    val destAirport = table.get(AIRPORT_DATA, key.arrivalAirportId.toString)
    val destCity = table.get(CITY_DATA, data(3))
    val origAirport = table.get(AIRPORT_DATA, data(1))
    val originCity = table.get(CITY_DATA, data(2))

    DelayedFlight(airline, head, origAirport, originCity, destAirport, destCity, arrivalDelay)
  }*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sorting").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    val value = broadcastVar.value
    val rawDataArray = sc.textFile(args(0)).map(line => line.split(","))
    val airlineData = rawDataArray.map(arr => createKeyValueTuple(arr))
    val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(1))
    keyedDataSorted.collect().foreach(println)

  }
}

object FlightKey {
  implicit def orderingByIdAirportIdDelay[A <: FlightKey]: Ordering[A] = {
    Ordering.by(fk => (fk.airLineId, fk.arrivalAirPortId, fk.arrivalDelay * -1))
  }
}

