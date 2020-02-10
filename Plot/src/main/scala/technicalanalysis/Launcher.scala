package technicalanalysis

import java.io.{File, FileWriter}

import com.typesafe.config.ConfigFactory
import scala.sys.process._


object Launcher {

  def init(stocks: List[String], startDate: String, endDate: String, nodes: Int, forecast: Boolean): Unit = {
    var stocksListString = ""
    for (i <- stocks.indices)
      stocksListString += stocks(i) + ".txt,"
    val fw = new FileWriter(new File("/home/spark/Plot/src/main/scala/technicalanalysis/launcher.sh"), false)
    fw.write("aws emr create-cluster --name \"SMTA\" --release-label emr-5.29.0 --instance-type m4.large --instance-count " + nodes + " --applications Name=Spark --steps Type=Spark,Name=\"Spark Program\",ActionOnFailure=TERMINATE_CLUSTER,Args=[--class,technicalanalysis.MainApp,s3://smta-data/smta_2.11-0.1.jar,\"" + startDate + "\",\"" + endDate + "\"," + forecast + "," + stocksListString.dropRight(1) + "] --log-uri s3://smta-data/log --use-default-roles --auto-terminate")
    fw.close()
  }

  def launch(): Unit = {
    val launcher = "/bin/sh /home/spark/Plot/src/main/scala/technicalanalysis/launcher.sh".!!
    Thread.sleep(2000)

    println("EMR Cluster creation output:")
    println(launcher)

    val parsed = ConfigFactory.parseString(launcher)
    val clusterId = if (parsed.getString("ClusterId") != "") parsed.getString("ClusterId") else ""
    if (clusterId == "") sys.exit()

    var completed = false
    var lastState = ""

    println("EMR Cluster description output:")
    while (!completed) {
      val checker = ("aws emr describe-cluster --cluster-id " + clusterId).!!
      Thread.sleep(5000)

      val checked = ConfigFactory.parseString(checker)
      val state = checked.getObject("Cluster.Status").get("State").render().dropRight(1).drop(1)
      if (state != lastState) {
        println("\n" + state)
        lastState = state
      }
      print(".....")
      completed = if (state contains "TERMINAT") true else false
    }

    if (completed)
      println("\n AWS EMR cluster execution completed.")
  }

}
