package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import sarah.spark.statistics.Statistics
import sarah.spark.statistics.Statistics._

object MonthCountInEvening {

  def valueByMonth(line: String): (String, Int) = {
    val fields = line.split(" ")
    if (fields.length <= 3) ("", 0) else {
      val dtFields = fields(3).split("/")
      if (dtFields.length <= 2) ("", 0) else {
        val hour = Integer.parseInt(dtFields(2).split(":")(1));
        if (hour >= 12) (dtFields(1), 1) else ("", 0)
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: MonthCountInEvening <master> <input><output> true|false")
      System.exit(1)
    }

    val sc = new SparkContext(args(0),
      "WordCount",
      System.getenv("SPARK_HOME"),
      SparkContext.jarOfClass(this.getClass).toSeq)

    val stats = new Statistics(sc, true, .05)

    sc.textFile(args(1)).
      initialize(stats).
      map(valueByMonth).
      filter(pair => pair._1.length() > 0)
      .computeStatistics(stats, "valueByMonth")
    .reduceByKey((x,y)=>x+y).
     sortByKey(true).
     take(100).foreach(println)
    //saveAsTextFile(args(2))
    println("Input size " +stats.numberRecordsInInput)  
    println("Sarah sample size " + stats.numberRecordsInSample)
    println("Sarah intermediate sample size " + stats.numberRecordsInIntermediate)
  }
}