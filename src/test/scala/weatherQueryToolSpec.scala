import java.io.File
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Aneesh on 22/02/2019.
  */

class weatherQueryToolSpec extends org.scalatest.FlatSpec {
  val location: List[String] = List("oxford")
  val destination = "src/test/weather_data/"
  val dtf = DateTimeFormatter.ofPattern("yyyy-MM")
  val txtExtension = ".txt"

  val conf = new SparkConf().setAppName("weatherdata").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)


  "A file" should "be downloaded from the met website" in {
    weatherQueryTool.downloadWeatherFiles(location,destination,txtExtension)
    val oxfordFile = new File(destination + "oxford" + txtExtension)
    assert(oxfordFile.exists())
    assert(oxfordFile.length() >= 104252)
  }

  "A file" should "be parsed correctly" in {
    val cleanedFileLines = weatherQueryTool.getFileAndRemoveHeader(destination, location.head)
    val oxfordFile = new File(destination + "oxford.txt")
    assert(cleanedFileLines.size >= 1993)
    assert(cleanedFileLines.head.trim == "1853   1    8.4     2.7       4    62.8     ---")
  }

  "A file" should "be tokenized correctly and parsed into weather instance" in {
    val tokenizedEntry = weatherQueryTool.cleanAndTokenizeEntry("   2018   7   27.1*   14.5*      ---   20.7*  300.7*  Provisional")
    assert(tokenizedEntry.size > 7)
    assert(tokenizedEntry(2) == "27.1")
    assert(!tokenizedEntry.contains("Provisional"))

    val weatherInstance : weatherInstance = weatherQueryTool.toWeatherInstance(location.head, tokenizedEntry, dtf)
    assert(weatherInstance.yearMonth.getYear == 2018)
    assert(weatherInstance.yearMonth.getMonthValue == 7)
    assert(weatherInstance.weatherStats.tmax.get == 27.1)
    assert(weatherInstance.weatherStats.afDays.isEmpty)
  }

  "A RDD" should "be created from the file and queried" in {
    //create RDD
    val RDD = weatherQueryTool.createSparkRDD(location, destination, dtf, sc)
    assert(RDD.count() == 1993)

    //query 1
    val queryResult1: RDD[(String, Long)] = weatherQueryTool.queryOne(RDD)
    val topResult1 = queryResult1.take(1)
    assert(topResult1.head._1 == location.head)
    assert(topResult1.head._2 == 166)

    //query 2
    val queryResult2: RDD[(String, Option[Double])] = weatherQueryTool.queryTwo(RDD)
    val topResult2 = queryResult2.take(1)
    assert(topResult2.head._1 == location.head)
    assert((topResult2.head._2.get - 108995.9).abs < 0.1)

    //query 3
    val queryResult3: RDD[(String, (YearMonth, Double))] = weatherQueryTool.queryThree(RDD)
    val topResult3 = queryResult3.take(1)
    assert(topResult3.head._1 == location.head)
    assert(topResult3.head._2._1.getYear == 1875 && topResult3.head._2._1.getMonthValue == 10)
    assert((topResult3.head._2._2 - 192.9).abs < 0.1)

    //query 4
    val queryResult4: RDD[(Int, Double)] = weatherQueryTool.queryFour(RDD)
    val topResult4 = queryResult4.take(1)
    assert(topResult4.head._1 == 1989 && (topResult4.head._2 - 300.8).abs < 0.1)
  }

}
