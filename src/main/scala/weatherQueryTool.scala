import java.io.{BufferedOutputStream, FileOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Aneesh on 22/02/2019.
  */
object weatherQueryTool extends App {

  //Setup new Spark instance
  val conf = new SparkConf().setAppName("weatherdata").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  val dtf = DateTimeFormatter.ofPattern("yyyy-MM")

  //manually list out locations which are being queried
  val fileDirectory: String = "src/main/resources/weather_data/"
  val txtExtension = ".txt"
  val locations: List[String] = List("aberporth", "armagh", "ballypatrick", "bradford",
    "braemar", "camborne", "cambridge", "cardiff", "chivenor", "cwmystwyth", "dunstaffnage", "durham", "eastbourne",
    "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars", "lowestoft", "manston", "nairn", "newtonrigg",
    "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton", "stornoway", "suttonbonington",
    "tiree", "valley", "waddington", "whitby", "wickairport", "yeovilton")


  downloadFiles(locations)

//create spark dataset
  val data: RDD[weatherInstance] = sc.parallelize(locations.map(x => (x,removeHeader(x)))
      .flatMap(z => z._2.map(record => cleanAndTokenizeEntry(z._1, record)).filter(b => b.length > 6)
        .map(a => toWeatherInstance(z._1,a))))

  //make this more spark-y

  //Query 1:
  //data.groupBy(_.location).map(entry => (entry._1, entry._2.minBy(_.yearMonth).yearMonth.until(entry._2.maxBy(_.yearMonth.getYear).yearMonth, ChronoUnit.YEARS))).sortBy(-_._2).foreach(pair => println(pair._1 + " " + pair._2 + " years"))

  //Query 2:
  //data.groupBy(_.location).map(a => (a._1, a._2.reduce(_.weatherStats.rain.getOrElse(0.0) + _.weatherStats.rain.getOrElse(0.0)))).sortBy(-_._2).foreach(c => println(c._1 + " " + c._2 + "cm rain"))
  //data.map(entry => (entry.location, entry.weatherStats.rain)).reduceByKey((a,b) => Some(a.getOrElse(0.0) + b.getOrElse(0.0))).sortBy(-_._2.get).collect().foreach(pair => println(pair._1 + " got " + pair._2.get + " cm rain"))

  //Query 2b:
  //data.map(entry => (entry.location, entry.weatherStats.sunHours)).reduceByKey((a,b) => Some(a.getOrElse(0.0) + b.getOrElse(0.0))).sortBy(-_._2.get).collect().foreach(pair => println(pair._1 + " got " + pair._2.get + " hours of sunshine"))

  //Query 3a
  data.map(entry => (entry.location, (entry.yearMonth, entry.weatherStats.rain.getOrElse(0.0)))).reduceByKey{case ((a,b),(c,d)) => (if (math.max(b,d) == b) a else c,math.max(b,d))}.collect().foreach(pair => println(pair._1 + " got " + pair._2._2 + " rainfall in " + pair._2._1 ))

  //Query 4:
  //data.filter(_.yearMonth.getMonthValue == 5).map(entry => (entry.location, entry.weatherStats.tmax)).reduceByKey((a,b) => Some(a.getOrElse(0.0) + b.getOrElse(0.0))).sortBy(-_._2.get).collect().foreach(pair => println(pair._1 + " got" + pair._2.get + " degrees heat"))

  //create directory to host weather data files, and download the above files into it
  def downloadFiles(locations: List[String]) = {
    new java.io.File(fileDirectory).mkdirs
    locations.map(location => {
      val url = new URL("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/" + location + "data" + txtExtension)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      val in: InputStream = connection.getInputStream
      val fileToDownloadAs = new java.io.File(fileDirectory + location + txtExtension)
      val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)
      out.flush()
      out.close()
    })
  }

  def removeHeader(location: String) : List[String] = {
    var i = 0 // This has to change
      while(!scala.io.Source.fromFile(fileDirectory + location + txtExtension).getLines().toList(i).trim.startsWith("yyyy")){
        i = i + 1
      }
    scala.io.Source.fromFile(fileDirectory + location + txtExtension).getLines.drop(i+2).toList
  }

  def cleanAndTokenizeEntry(location: String, uncleanEntry: String) : List[String] ={
    uncleanEntry.trim.split(" ").toList.map(_.trim).filter(_ != "").map(_.replaceAll("(\\*|\\$|#)", "")).map(_.replace("Provisional", ""))
    //if (cleanedOuput.length < 7) println("Malformed record in  " + location + " = " + uncleanEntry)

  }

  def toWeatherInstance(fileName: String, weatherData: List[String]) : weatherInstance = {
      weatherInstance(location = fileName,
        yearMonth = YearMonth.parse(weatherData.head + "-" + (if (weatherData(1).length < 2) "0" + weatherData(1) else weatherData(1)), dtf), //TODO:probably should catch this
        weatherStats = new weatherStats(
          tmax = numberOrNone(weatherData(2)),
          tmin = numberOrNone(weatherData(3)),
          afDays = numberOrNone(weatherData(4)),
          rain = numberOrNone(weatherData(5)),
          sunHours = numberOrNone(weatherData(6))))
  }

  def numberOrNone(number: String): Option[Double] = {
    try {
      if (number == "---") None else Some(number.toDouble)
    } catch {
      case error: NumberFormatException => {
        //println("Failed to parse " + number)
        None
      }
      case error: Exception => {
        //println("Failed to parse - " + number)
        error.printStackTrace()
        throw error
      }
    }
  }

}

//Weather object which will be queried
case class weatherInstance(location: String,
                           yearMonth: YearMonth,
                           weatherStats: weatherStats)

//Weather stats are all optional since they may not be defined
case class weatherStats(tmax: Option[Double],
                        tmin: Option[Double],
                        afDays: Option[Double],
                        rain: Option[Double],
                        sunHours: Option[Double])