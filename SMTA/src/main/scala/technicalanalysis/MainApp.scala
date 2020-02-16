package technicalanalysis

import java.io.File

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import technicalanalysis.indicators.{IndicatorADX, IndicatorBB, IndicatorMACD, IndicatorOBV, IndicatorRSI}

import scala.reflect.io.Directory

case class Record (date: DateTime, open: Double, high: Double, low: Double, close: Double, volume: Long) {
  override def toString: String = s"$date: ($open, $high, $low, $close) - $volume"
}

trait IndicatorValue {
  def date: DateTime
}

case class LongIndicatorValue(override val date: DateTime, value: Long) extends IndicatorValue {
  override def toString: String = s"$date: ($value)"
  def + (that: LongIndicatorValue): LongIndicatorValue = LongIndicatorValue(date, this.value + that.value)
}

case class DoubleIndicatorValue(override val date: DateTime, value: Double) extends IndicatorValue {
  override def toString: String = s"$date: ($value)"
  def + (that: DoubleIndicatorValue): DoubleIndicatorValue = DoubleIndicatorValue(date, this.value + that.value)
}

// A simple partitioner that puts each row in the partition we specify by the key
class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
  def numPartitions = p
  def getPartition(key: Any) = key.asInstanceOf[Int]
}


object MainApp {

  final val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Stock Market Technical Analysis")
    .getOrCreate()

  def main(args: Array[String]) {
    spark.sparkContext.setLogLevel("ERROR")
    val PATH = "src/main/resources/stocks/"
   // val PATH = "s3://smta-data/stocks/"
    val PERIOD = 14
    val FORECAST = 42
    val BEGIN = DateTime.parse("1900-01-01")
    val END = DateTime.parse("2100-01-01")
    val FORECAST_ENABLED =  false
    val STOCK_ACCURACY = Array(0.0D, 0.0D, 0.0D, 0.0D, 0.0D) // RSI, MACD, ADX, BB, time
    val d = new File("src/main/resources/stocks/")
    //val stocks = d.listFiles.filter(_.isFile).map(filename=> filename.getName).toList
    val stocks = spark.sparkContext.textFile("src/main/scala/technicalanalysis/file-names.txt").collect().toList
    //print(stocks)
    //val stocks = List("a.us.txt")
    //val stocks = List("a.us.txt", "aa.us.txt", "aaba.us.txt", "aame.us.txt","aan.us.txt", "aaon.us.txt", "aap.us.txt", "aapl.us.txt", "aau.us.txt", "aav.us.txt", "aaww.us.txt", "aaxn.us.txt", "ab.us.txt", "abax.us.txt", "abb.us.txt")
    //val stocks = List("a.us.txt", "aa.us.txt", "aaba.us.txt", "aame.us.txt","aan.us.txt", "aaon.us.txt")
    val DIR = new Directory(new File("src/main/resources/plot/"))
    DIR.deleteRecursively()
    DIR.createDirectory()

    //for (i <- 4 until args.length) stocks = stocks :+ args(i).toString
    val startTime = System.currentTimeMillis()
    var startTimeTempForAll:Long = 0
    var startTimeADX:Long = 0
    var startTimeBB:Long = 0
    var startTimeMACD:Long = 0
    var startTimeOBV:Long = 0
    var startTimeRSI:Long = 0

    var count = 0
    stocks.par.foreach { stock =>
      count = count + 1
      if(count%100==0) println("Computed already: " + count)
      println(stock.toUpperCase())

      val baseRdd = spark.sparkContext.textFile(PATH + stock). // Load file to RDD
        mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}. // Remove header
        map(_.split(",")). // Split the values by comma character
        map { // Cast rows to Record
          case Array(date, open, high, low, close, volume, openInt) =>
            Record(DateTime.parse(date), open.toDouble, high.toDouble, low.toDouble, close.toDouble, volume.toLong)
        }

      // Get data in specified range
      val rdd = baseRdd.
        //filter(record => record.date.isAfter(BEGIN) && record.date.isBefore(END)). // Get data in the chosen time window (begin and end excluded)
        zipWithIndex // Give and index to each element
        .map(record => (record._2, record._1)) // Set index as the key

      if (!rdd.isEmpty()) {

        // Create stock directory if does not exist
        val directory = new File("src/main/resources/plot/" + stock.dropRight(4))
        //val directory = new File("ss3://smta-data/plot/" + stock.dropRight(4))
        if(!directory.exists) directory.mkdir

        // Plot stock price
        Utils.writeToFile(stock.dropRight(4) + "/PRICE", rdd.map(x => x._1 + ", " + x._2.close))

        // ---------- INDICATORS ----------

        println("Compute indicators")

        // OBV Indicator
        startTimeTempForAll = System.currentTimeMillis()
        IndicatorOBV.computeOBV(stock.dropRight(4), rdd)
        startTimeOBV = startTimeOBV + (System.currentTimeMillis()-startTimeTempForAll)

        // RSI Indicator
        startTimeTempForAll = System.currentTimeMillis()
        IndicatorRSI.computeRSI(stock.dropRight(4), rdd, PERIOD)
        startTimeRSI = startTimeRSI + (System.currentTimeMillis()-startTimeTempForAll)

        // MACD Indicator
        startTimeTempForAll = System.currentTimeMillis()
        IndicatorMACD.computeMACD(stock.dropRight(4), rdd.map(_._2))
        startTimeMACD = startTimeMACD + (System.currentTimeMillis()-startTimeTempForAll)

        //try {
          // ADX Indicator
          //println("ADX counter" + rdd.map(_._2).count())
        startTimeTempForAll = System.currentTimeMillis()
        IndicatorADX.computeADX(stock.dropRight(4), rdd.map(_._2))
        startTimeADX = startTimeADX + (System.currentTimeMillis()-startTimeTempForAll)

        /*}catch {
          case _: AssertionError => println("FILE ROTTO:" + stock) // Expected, so continue
          case _: Exception => println("FILE ROTTO:" + stock) // Expected, so continue
        }*/

        // BB Indicator
        startTimeTempForAll = System.currentTimeMillis()
        IndicatorBB.computeBB(stock.dropRight(4), rdd.map(_._2))
        startTimeBB = startTimeBB + (System.currentTimeMillis()-startTimeTempForAll)

        // ---------- FORECAST ----------

        if (FORECAST_ENABLED) {
          println("Compute forecast accuracy")

          // RSI Forecast
          val rsiAccuracy = IndicatorRSI.forecastRSI(baseRdd.zipWithIndex().map(record => (record._2, record._1)), PERIOD, FORECAST)
          println("RSI accuracy = " + rsiAccuracy)
          STOCK_ACCURACY(0) += rsiAccuracy

          // MACD Forecast
          val macdAccuracy = IndicatorMACD.forecastMACD(baseRdd.zipWithIndex().map(record => (record._2, record._1)).map(_._2), PERIOD, FORECAST)
          println("MACD accuracy = " + macdAccuracy)
          STOCK_ACCURACY(1) += macdAccuracy

          // ADX Forecast
          val adxAccuracy = IndicatorADX.forecastADX(baseRdd.zipWithIndex().map(record => (record._2, record._1)).map(_._2), PERIOD, FORECAST)
          println("ADX accuracy = " + adxAccuracy)
          STOCK_ACCURACY(2) += adxAccuracy

          // BB Forecast
          val bbAccuracy = IndicatorBB.forecastBB(baseRdd.zipWithIndex().map(record => (record._2, record._1)).map(_._2), PERIOD, FORECAST)
          println("BB accuracy = " + bbAccuracy)
          STOCK_ACCURACY(3) += bbAccuracy
        }
      } else
        println("No data in or less data " +
          (PATH + stock) + " within time-window [" +
          BEGIN.dayOfMonth().get() + "/" + BEGIN.monthOfYear().get() + "/" + BEGIN.year().get() + " - " +
          END.dayOfMonth().get() + "/" + END.monthOfYear().get() + "/" + END.year().get() + "]")
    }

    STOCK_ACCURACY(4) = (System.currentTimeMillis() - startTime) / 1000 / 60 // minutes
    //STOCK_ACCURACY(4) = (System.currentTimeMillis() - startTime) / 1000.0 // minutes
    Utils.writeToFile("accuracy", spark.sparkContext.parallelize(STOCK_ACCURACY).zipWithIndex.map {
      case (value, index) =>
        if (index == 0)
          "RSI Accuracy = " + (value / stocks.length.toDouble) + "%"
        else if (index == 1)
          "MACD Accuracy = " + (value / stocks.length.toDouble) + "%"
        else if (index == 2)
          "ADX Accuracy = " + (value / stocks.length.toDouble) + "%"
        else if (index == 3)
          "BB Accuracy = " + (value / stocks.length.toDouble) + "%"
        else if (index == 4)
          "Total execution time = " + value + "m"
        else
          "Error"
    })

    println("ADX time: " + startTimeADX/1000,
      "BB time: " + startTimeBB/1000, "RSI time: " + startTimeRSI/1000, "OBV time: " + startTimeOBV/1000, "MACD time: " + startTimeMACD/1000)


    spark.sparkContext.stop()
  }

}