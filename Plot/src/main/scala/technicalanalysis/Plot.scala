package technicalanalysis

import java.io.{BufferedReader, BufferedWriter, File, FileWriter, InputStreamReader}

import scala.reflect.io.Directory
import java.nio.file.{Files, Paths}

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.geometry.{Insets, Pos}
import scalafx.scene.Scene
import scalafx.scene.chart.{LineChart, NumberAxis, XYChart}
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.{Alert, Button, CheckBox, Label, Tab, TabPane, TextField}
import scalafx.scene.layout.{BorderPane, GridPane, HBox, Pane, VBox}
import technicalanalysis.Plot.{AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN}


object Plot extends JFXApp {

  final val AWS_ACCESS_KEY = "---"
  final val AWS_SECRET_ACCESS_KEY = "---"
  final val AWS_SESSION_TOKEN = "---"
  final val CHART_WIDTH = 1500
  final val CHART_HEIGHT = 750

  var forecast = false
  var indicators = Map("PRICE" -> false, "ADX" -> false, "BB_LOW" -> false,"BB_HIGH" -> false,"BB_MEAN" -> false, "MACD_NORMAL" -> false, "MACD_SIGNAL" -> false, "RSI" -> false,"OBV" -> false)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Stock Market Technical Analysis Plot")
    .getOrCreate()

  val amazonS3Client = AmazonS3Client.builder()
    .withCredentials(new AWSStaticCredentialsProvider(new BasicSessionCredentials(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)))
    .withRegion(Regions.US_EAST_1).build()

  // PLOT CHARTS
  stage = new PrimaryStage {
    title = "Stock Price Line Chart"
    scene = new Scene(CHART_WIDTH, CHART_HEIGHT) {
      stylesheets.add("style.css")

      //STOCKS
      val stocksLabel = new Label {
        text = "Insert here the stocks to monitor:"
        minWidth = CHART_WIDTH * 0.80
        minHeight = CHART_HEIGHT * 0.10
        style = "-fx-font: normal bold 12pt sans-serif"
      }

      val stocksTextBox = new TextField {
        minWidth = CHART_WIDTH * 0.80
        minHeight = 50
        style = "-fx-background-color: #4c4949; -fx-font: normal bold 12pt sans-serif; -fx-text-inner-color: white;"
        alignment = Pos.CenterLeft
      }

      val indicatorsLabel = new Label {
        text = "Choose which indicators compute for the selected stocks:"
        minWidth = CHART_WIDTH * 0.80
        minHeight = CHART_HEIGHT * 0.09
        style = "-fx-font: normal bold 12pt sans-serif"
      }

      val box = new HBox {
        children = Seq(
          new CheckBox{
            text = "ADX"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("ADX")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "BB_LOW"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("BB_LOW")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "BB_MEAN"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("BB_MEAN")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "BB_HIGH"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("BB_HIGH")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "MACD_NORM"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("MACD_NORMAL")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "MACD__SIGN"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("MACD_SIGNAL")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "RSI"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("RSI")
            alignment = Pos.TopLeft
          },
          new CheckBox{
            text = "OBV"
            minWidth = CHART_WIDTH * 0.09
            onAction = _ => setIndicator("OBV")
            alignment = Pos.TopLeft
          }
        )
        minWidth = CHART_WIDTH * 0.60
        minHeight = 50
      }

      val startDateLabel = new Label {
        text = "Starting date to monitor the selected stocks (format: YYYY-MM-dd)"
        minWidth = CHART_WIDTH * 0.80
        minHeight = 50
        style = "-fx-font: normal bold 12pt sans-serif"
      }

      val startDate = new TextField() {
        minWidth = 200
        minHeight = 50
        alignment = Pos.CenterLeft
        style = "-fx-background-color: #4c4949; -fx-font: normal bold 12pt sans-serif; -fx-text-inner-color: white;"
      }

      val endDateLabel = new Label {
        text = "Ending date to monitor the selected stocks (format: YYYY-MM-dd)"
        minWidth = CHART_WIDTH * 0.80
        minHeight = 50
        style = "-fx-font: normal bold 12pt sans-serif"
      }

      val endDate =  new TextField() {
        minWidth = 200
        minHeight = 50
        alignment = Pos.CenterLeft
        style = "-fx-background-color: #4c4949; -fx-font: normal bold 12pt sans-serif; -fx-text-inner-color: white;"
      }

      val check_forecast = new CheckBox{
        text = "Compute forecast accuracy"
        minWidth = CHART_WIDTH * 0.03
        minHeight = 30
        onAction = _ => forecast = !forecast
        alignment = Pos.CenterLeft
        style = "-fx-font: normal bold 12pt sans-serif; -fx-text-inner-color: white;"
        margin = Insets(15, 15, 15, 15)
      }

      val button = new Button {
        text = "Compute"
        minWidth = CHART_WIDTH * 0.80
        minHeight = 50
        //padding = Insets(20, CHART_WIDTH*0.20, 15, CHART_WIDTH*0.20)
        alignment = Pos.Center
        style = "-fx-background-color: #4c4949; -fx-font: normal bold 12pt sans-serif"
        onAction = _ => {
          if(stocksTextBox.getText.isEmpty) {
            new Alert(AlertType.Error) {
              initOwner(owner)
              title = "No stock chosen"
              headerText = "Specify at least one stock"
              contentText = "Please select at least one stock, multiple stocks can be separated by comma."
            }.showAndWait()
          } else if(indicators.count(item =>item._2.equals(true)) == 0) {
            new Alert(AlertType.Error) {
              initOwner(owner)
              title = "No indicator selected"
              headerText = "Specify at least one indicator"
              contentText = "Please select at least one indicator"
            }.showAndWait()
          }
          else { // If everything is ok, plot the results
            var startDateDT:DateTime = null
            var endDateDT:DateTime = null
            try {
              val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
              setIndicator("PRICE") //set to true
              startDateDT = dtf.parseDateTime(startDate.getText)
              endDateDT = dtf.parseDateTime(endDate.getText)
            } catch {
              case x: Exception => new Alert(AlertType.Error) {
                println(x)
                initOwner(owner)
                title = "Invalid date format"
                headerText = "Please use the correct date format"
                contentText = "Date format: YYYY-MM-dd"
              }.showAndWait()
            }
            // Set Parameters
            val stockNames = stocksTextBox.getText.split(",").map(_.trim).toList

            // Compute stocks with AWS
            val awsBackend = Launcher
            awsBackend.init(stockNames, startDate.getText, endDate.getText, 8, forecast)

            new Alert(AlertType.Information) {
              initOwner(owner)
              title = "Creating and launching AWS cluster"
              headerText = "AWS is computing..."
              contentText = "Please wait, the result will appear in a few minutes."
            }.showAndWait()

            awsBackend.launch()
            //TODO: CALL HERE A METHOD: SAVE CSVS
            save_csv_locally(startDateDT,
              endDateDT,
              stockNames,
              indicators.keys.toList) //pass all because i need all calculated files
            // It's all ok
            changeScene(startDateDT,
              endDateDT,
              stockNames,
              indicators.filter(item => item._2).keys.toList,forecast)
          }
        }
      }

      val pane = new GridPane {
        minWidth = CHART_WIDTH
        minHeight = CHART_HEIGHT
        add(stocksLabel,0,1)
        add(stocksTextBox,0,2)
        add(indicatorsLabel, 0,3)
        add(box,0,4)
        add(startDateLabel,0,5)
        add(startDate,0,6)
        add(endDateLabel,0,7)
        add(endDate,0,8)
        add(check_forecast, 0, 9)
        add(button,0,10)
        alignment = Pos.Center
      }

      root = pane
    }

    scene
  }

  def setIndicator(indicator: String): Unit = {
    indicators = indicators + (indicator -> !indicators(indicator))
  }


  def save_csv_locally(startDate: DateTime, endDate: DateTime, stocks: List[String], indicators_to_calculate: List[String]): Unit = {

    for (i <- stocks.indices) {
      val directory = new Directory(new File("src/main/resources/plot/"+ stocks(i) + "/"))
      directory.deleteRecursively()
      //Create new
      new java.io.File( "src/main/resources/plot/"+ stocks(i)).mkdirs
      for (j <- indicators_to_calculate.indices) {
        //Create new
        println("plot/" + stocks(i) + "/" + indicators_to_calculate(j) + "/part-00000")
        val obj = amazonS3Client.getObject("smta-data", "plot/" + stocks(i) + "/" + indicators_to_calculate(j) + "/part-00000")
        val reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()))
        var line = reader.readLine
        var rawData = Seq[String]()
        while (line != null) {
          rawData = rawData :+ line + "\n"
          line = reader.readLine
        }
        val file = new File("src/main/resources/plot/"+ stocks(i) + "/" + indicators_to_calculate(j)+".csv")
        val bw = new BufferedWriter(new FileWriter(file))
        rawData.foreach(line => bw.write(line))
        bw.close()
      }
    }
  }

  def changeScene(startDate: DateTime, endDate: DateTime, stocks: List[String], indicators_to_calculate: List[String],forecast:Boolean): Unit = {
    stage.scene = new Scene(CHART_WIDTH, CHART_HEIGHT) {
      stylesheets.add("style.css")

      val stockTabs = new TabPane

      for (i <- stocks.indices) {
        val xAxis = new NumberAxis()
        xAxis.label = "Days"

        val yAxis = new NumberAxis()
        yAxis.label = "Values"

        val chart = new LineChart(xAxis, yAxis)
        chart.title = "Stock \"" + stocks(i).toUpperCase + "\" Chart"
        chart.minWidth = CHART_WIDTH * 0.98
        chart.minHeight = CHART_HEIGHT * 0.95

        for (j <- indicators_to_calculate.indices) {
          val indicator_stock = spark.sparkContext.textFile("src/main/resources/plot/" + stocks(i) + "/" + indicators_to_calculate(j)+".csv").collect()

          val dataT =  indicator_stock.
            map(_.split(",")). // Split the values by comma character
            map { case Array(x, y) => (x.toDouble, y.toDouble) }

          val max = dataT.reduce((acc, value) => { if (acc._2 < value._2) value else acc })
          val min = dataT.reduce((acc, value) => { if (acc._2 > value._2) value else acc })
          val data = dataT.map(x => (x._1, (x._2 - min._2) / (max._2 - min._2))) // Move data in range [0, 1]
          //get all charts
          chart.getData.add(
            XYChart.Series[Number, Number](indicators_to_calculate(j), ObservableBuffer(data.map(td => XYChart.Data[Number, Number](td._1, td._2)):_*))
          )
        }

        //Checkboxes index
        val box = new VBox() {
          children = Seq(
            new CheckBox{
              text = "ADX"
              selected = indicators_to_calculate.contains("ADX")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => setIndicator("ADX")
              alignment = Pos.TopLeft
              onAction = _ => {
                setIndicator("ADX")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
            },
            new CheckBox{
              text = "BB__LOW"
              selected = indicators_to_calculate.contains("BB_LOW")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("BB_LOW")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "BB__MEAN"
              selected = indicators_to_calculate.contains("BB_MEAN")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("BB_MEAN")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "BB__HIGH"
              selected = indicators_to_calculate.contains("BB_HIGH")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("BB_HIGH")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "MACD__NORM"
              selected = indicators_to_calculate.contains("MACD_NORMAL")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("MACD_NORMAL")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "MACD__SIGN"
              selected = indicators_to_calculate.contains("MACD_SIGNAL")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("MACD_SIGNAL")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "RSI"
              selected = indicators_to_calculate.contains("RSI")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("RSI")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            },
            new CheckBox{
              text = "OBV"
              selected = indicators_to_calculate.contains("OBV")
              minWidth = CHART_WIDTH * 0.09
              onAction = _ => {
                setIndicator("OBV")
                changeScene(startDate, endDate, stocks,indicators.filter(item => item._2).keys.toList,forecast:Boolean)
              }
              alignment = Pos.TopLeft
            }
          )
          minWidth = CHART_WIDTH * 0.10
          minHeight = CHART_HEIGHT
          alignment = Pos.CenterLeft
          margin = Insets(0,15,0,15)
        }

        //Charts
        val chartPane = new GridPane {
          minWidth = CHART_WIDTH
          minHeight = CHART_HEIGHT
          add(chart,0,1)
          add(box,1,1)
          alignment = Pos.Center
        }

        val tab = new Tab
        tab.text = stocks(i).toUpperCase
        tab.closable = false

        tab.content = chartPane
        stockTabs += tab
      }

      val rootPane = new BorderPane
      rootPane.top = stockTabs
      root = rootPane
    }
  }

}