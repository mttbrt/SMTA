package technicalanalysis

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import technicalanalysis.MainApp.spark

object Utils {

  def linearRegression(trainingData: RDD[(Long, Record)]): Boolean = {
    val rowRDD = trainingData.map(x => Row(x._2.close, Vectors.dense(x._1.toDouble)))
    val schema = StructType(StructField("label", DoubleType, true) :: StructField("features", org.apache.spark.ml.linalg.SQLDataTypes.VectorType, true) :: Nil)
    val training = spark.createDataFrame(rowRDD, schema)

    val lr = new LinearRegression()
      .setMaxIter(25)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // If the slope of the line is positive return true, else false
    lrModel.coefficients(0) >= 0
  }

  def writeToFile(filename: String, data: RDD[String]): Unit = {
    //data.coalesce(1).saveAsTextFile("s3://smta-data/plot/" + filename)
    data.coalesce(1).saveAsTextFile("src/main/resources/plot/" + filename)
  }

  def roundToDecimal(value: Double): Double = {
    val s = math pow (10, 2); (math round value * s) / s
    s
    //BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    println("Elapsed time: " + ((t1 - t0) / 1000000) + "ms")
    result
  }

}
