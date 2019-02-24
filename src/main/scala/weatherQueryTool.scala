import java.io._
import java.net.{HttpURLConnection, URL}
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import org.apache.commons.logging.{Log, LogFactory}
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
  val weatherFilesDirectory: String = "src/main/resources/weather_data/"
  val resultsDirectory: String = "src/main/resources/results/"
  val txtExtension = ".txt"
  val logger = LogFactory.getLog(this.getClass.getName)

  //manually list out locations which are being queried
  val locations: List[String] = List("aberporth", "armagh", "ballypatrick", "bradford",
    "braemar", "camborne", "cambridge", "cardiff", "chivenor", "cwmystwyth", "dunstaffnage", "durham", "eastbourne",
    "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars", "lowestoft", "manston", "nairn", "newtonrigg",
    "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton", "stornoway", "suttonbonington",
    "tiree", "valley", "waddington", "whitby", "wickairport", "yeovilton")

  //download data from the internet and parse into Spark RDD
  logger.info("Starting downloads to directory - " + weatherFilesDirectory)
  downloadWeatherFiles(locations, weatherFilesDirectory)
  logger.info("All downloads complete")
  val data = createSparkRDD(locations)
  logger.info("RDD Created")

  //create results directory
  val resultsDir = new java.io.File(resultsDirectory)
  if (!resultsDir.exists()) resultsDir.mkdirs

  logger.info("Running query 1 - Amount of time each station has been/ was online for")
  //Query 1: Amount of time each station has been/ was online for
  val file1 = new File(resultsDirectory + "query1.txt")
  val bw1 = new BufferedWriter(new FileWriter(file1))
  queryOne(data).foreach(pair => bw1.write(pair._1 + " " + pair._2 + " years\n"))
  bw1.close()

  logger.info("Running query 2 - Total rainfall in one location")
  //Query 2: Total rainfall in one location  - stat can be changed
  val file2 = new File(resultsDirectory + "query2.txt")
  val bw2 = new BufferedWriter(new FileWriter(file2))
  queryTwo(data).foreach(pair => bw2.write(pair._1 + " got " + pair._2.getOrElse(0.0) + " cm rain\n"))
  bw2.close()

  logger.info("Running query 3 - Month and year in which each location got max rainfall")
  //Query 3: Month and year in which each location got max rainfall - stat can be changed
  val file3 = new File(resultsDirectory + "query3.txt")
  val bw3 = new BufferedWriter(new FileWriter(file3))
  queryThree(data).foreach(pair => bw3.write(pair._1 + " got " + pair._2._2 + " cm rainfall in " + pair._2._1 + "\n"))
  bw3.close()

  logger.info("Running query 4 - Average of stats in May across all locations")
  //Query 4: Average of stats in May across all locations - done for sunshine hours, can be changed
  val file4 = new File(resultsDirectory + "query4.txt")
  val bw4 = new BufferedWriter(new FileWriter(file4))
  queryFour(data).foreach(pair => bw4.write(pair._1 + " averaged " + pair._2+ " hours of sunshine \n"))
  bw4.close()

  //create directory to host weather data files, and download the above files into it
  def downloadWeatherFiles(locations: List[String], destination: String, fileExtension: String = txtExtension) = {
    new java.io.File(destination).mkdirs
    locations.map(location => {
      val url = new URL("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/" + location + "data" + fileExtension)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      val in: InputStream = connection.getInputStream
      val fileToDownloadAs = new java.io.File(destination + location + fileExtension)
      val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)
      out.flush()
      out.close()
    })
  }

  def getFileAndRemoveHeader(weatherFilesDirectory: String, location: String, txtExtension: String = txtExtension) : List[String] = {
    var index = 0
    val lines = scala.io.Source.fromFile(weatherFilesDirectory + location + txtExtension).getLines.toList
      while(!lines(index).trim.startsWith("yyyy")){
        index += 1
      }
    lines.drop(index+2)
  }

  def cleanAndTokenizeEntry(uncleanEntry: String) : List[String] ={
    val tokens = uncleanEntry.trim.split(" ").toList.map(_.trim).filter(_ != "").map(_.replaceAll("(\\*|\\$|#)", "")).map(_.replace("Provisional", ""))
    if (tokens.length < 7) logger.warn("Found malformed record = " + uncleanEntry)
    tokens
  }

  def toWeatherInstance(fileName: String, weatherData: List[String], dtf: DateTimeFormatter = dtf) : weatherInstance = {
      weatherInstance(location = fileName,
        yearMonth = YearMonth.parse(weatherData.head + "-" + (if (weatherData(1).length < 2) "0" + weatherData(1) else weatherData(1)), dtf),
        weatherStats = new weatherStats(
          tmax = doubleToOption(weatherData(2)),
          tmin = doubleToOption(weatherData(3)),
          afDays = doubleToOption(weatherData(4)),
          rain = doubleToOption(weatherData(5)),
          sunHours = if (weatherData.size > 6) doubleToOption(weatherData(6)) else None))
  }

  def doubleToOption(number: String): Option[Double] = {
    try {
      if (number == "---") None else Some(number.toDouble)
    } catch {
      case error: NumberFormatException => {
        logger.warn("Found invalid number = " + number)
        None
      }
      case error: Exception => {
        logger.error(error.printStackTrace())
        throw error
      }
    }
  }

  def createSparkRDD(locations: List[String], weatherFilesDirectory: String = weatherFilesDirectory, dtf: DateTimeFormatter = dtf, sc: SparkContext = sc, fileExtension: String = txtExtension) : RDD[weatherInstance] = {
    sc.parallelize(
      locations.map(location => (location,getFileAndRemoveHeader(weatherFilesDirectory, location, fileExtension))) //retrieves the location file, and removes header lines
        .flatMap(locationAndWeatherData => locationAndWeatherData._2.map(record => cleanAndTokenizeEntry(record)) //returns tokenized, cleaned records along with the station name
        .filter(dataInstance => dataInstance.length > 5) //filters out malformed data instances but not incomplete records
        .map(cleanedRecord => toWeatherInstance(locationAndWeatherData._1,cleanedRecord, dtf)))) //returns a RDD of weather instances
  }

  def queryOne(data: RDD[weatherInstance]) : RDD[(String, Long)] = {
    data.groupBy(_.location)
      .map(entry => (entry._1, entry._2.minBy(_.yearMonth).yearMonth.until(entry._2.maxBy(_.yearMonth.getYear).yearMonth, ChronoUnit.YEARS)))
      .sortBy(-_._2) //inverse sort
  }

  def queryTwo(data: RDD[weatherInstance]) : RDD[(String, Option[Double])] = {
    data.map(entry => (entry.location, entry.weatherStats.rain))
      .reduceByKey((a,b) => Some(a.getOrElse(0.0) + b.getOrElse(0.0)))
      .sortBy(-_._2.get) //inverse sort
  }

  def queryThree(data: RDD[weatherInstance]) : RDD[(String, (YearMonth, Double))] = {
    data.map(entry => (entry.location, (entry.yearMonth, entry.weatherStats.rain.getOrElse(0.0))))
      .reduceByKey{case ((a,b),(c,d)) => (if (math.max(b,d) == b) a else c,math.max(b,d))} //find Max of values
  }

  def queryFour(data: RDD[weatherInstance]): RDD[(Int, Double)] = {
    data.filter(_.yearMonth.getMonthValue == 5) //Filter out May records
      .map(entry => (entry.yearMonth.getYear(), (entry.weatherStats.sunHours.getOrElse(0.0), if (entry.weatherStats.sunHours.isDefined) 1 else 0)))
      .reduceByKey{case ((a,b),(c,d)) => (a+c, b+d)}
      .filter(_._2._2 != 0) //Filter out 0 records to avoid division by zero
      .map(value => (value._1, value._2._1/value._2._2))
      .sortBy(-_._2)
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