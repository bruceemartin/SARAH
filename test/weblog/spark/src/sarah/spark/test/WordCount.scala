package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: WordCount <master> <input><output>")
       System.exit(1)
     }

     val sc = new SparkContext(args(0),
       "WordCount",
       System.getenv("SPARK_HOME"),
       SparkContext.jarOfClass(this.getClass).toSeq)

     sc.textFile(args(1)).
     	flatMap(line => line.split("\\W")).
     	map(w => (w,1)).
     	filter(word => word._1.trim().length()>0).
     	reduceByKey((x,y)=>x+y).
     	sortByKey(true).
     	saveAsTextFile(args(2))

   }
 }