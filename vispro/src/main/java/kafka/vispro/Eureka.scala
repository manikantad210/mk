package kafka.vispro

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

object Eureka {

  def matchh(x: Int): String = {
    x match {
      case 0  => "zero"
      case 1  => "one"
      case 10 => "ten"
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("raefing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val rdd = sc.textFile("C:/Users/KOGENTIX/Desktop/eurekasampledata.txt")
    val co = new Configuration
    val dataArra = rdd.map(p => p.split(","))
    val f = rdd.map(p => p.split(",")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20), p(21), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30), p(31), p(32), p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), p(43), p(44)))
    val str = "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45"
    println("length --------------------------schema----> " + str.split(",").length)
    val schema = new StructType(str.split(",").map(a => StructField(a, StringType, true)))
    import sql.implicits._
    val df = sql.createDataFrame(f, schema)
    val makeString = df.map(s => s.mkString(","))
    makeString.foreach { s =>
      println(s)
    }
    def twice(op: Double => Double)(x: Double) = op(op(op(x)))
    val ddd = twice(_ * 3)(3)

    df.printSchema()
    val stru = df.schema
    stru.map { x => println(x.name) }
    df.registerTempTable("hhhhhhh")
    sql.sql("select sum(cast(c1 as Long)) as summmm from hhhhhhh").show()

    val x = 10
    val b = x.isInstanceOf[Int]
    println(x.asInstanceOf[Double])

    val y = "10"

    println(matchh(1))
    val a = Array(
      Map("age" -> null, "name" -> "Michael"),
      Map("age" -> 30, "name" -> "Andy"),
      Map("age" -> 19, "name" -> "Justin"))

    a.map { s =>
      s.getOrElse("age", "NULL")
    }

  }
  matchh(10)

}
