package kafka.vispro

import org.apache.spark.Partitioner

class AirlineFlightPartitioner(partitions: Int) extends Partitioner with Serializable{
    
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
  require(partitions == 0, s"No partitions")

    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[FlightKey]
      k.airLineId.hashCode() % numPartitions
  
    }
  }
