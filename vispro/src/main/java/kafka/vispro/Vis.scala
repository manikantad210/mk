package kafka.vispro

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

case class Calls(city: String, state: String, accountNumber: String, dayofCall: String, duration: Int)

case class CallsKey(city: String, dayofCall: String, count: Int)
object Vis {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Top Citiys").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataDuration = "C:/Users/KOGENTIX/Desktop/Newfolder/scenariodataduration.txt"
    val dataCount = "C:/Users/KOGENTIX/Desktop/Newfolder/scenariodatacount.txt"
    val rawDataArray = sc.textFile(dataDuration).map(line => line.split(","))
    val callsData = rawDataArray.map(s => Calls(s(0), s(1), s(2), s(3), s(4).toInt))

    val kvFor = for (call <- callsData) yield {
      ((call.city, call.dayofCall), call)
    }

    import sqlContext.implicits._
    val df1 = sc.parallelize(Seq((1, "mani"), (2, "kanta"))).toDF("idd", "name")
    val df2 = sc.parallelize(Seq((1, "manikanta"), (3, "kantaD"))).toDF("idd", "name")

    for (i <- 1 to 10 if i % 2 == 0) println(i)

    var con = true
      def med(r1: Row, r2: Row) = {
      if(r1.getAs[Int]("id") == r2.getAs[Int]("id")){
        r2
        
      }else{
        r1
      }
    }  
  

    val da = kvFor.countByKey()
    var arr = da.toArray
    println(da)

    import sqlContext.implicits._
    val callsDataDF = callsData.toDF
    callsDataDF.registerTempTable("callsData")

    sqlContext.sql("select city, count(*)  as cnt from callsData group by  city ").show
    val top10basedoncall = sqlContext.sql("select distinct(city) ,count(city) as cnt from callsData group by city order by cnt desc ").show

    val top10basedondayofcall = sqlContext.sql("select city , dayofCall, count(dayofCall) as cnt from callsData group by dayofCall, city order by cnt desc limit 10").show

    val top10basedonduration = sqlContext.sql("select  accountNumber, duration  from callsData order by duration desc limit 10").show

  }
}