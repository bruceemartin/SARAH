package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object IPCount {

   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: IPCount <master> <input><output>")
       System.exit(1)
     }

     val sc = new SparkContext(args(0),
       "WordCount",
       System.getenv("SPARK_HOME"),
       SparkContext.jarOfClass(this.getClass).toSeq)

     sc.textFile(args(1)).
     	map(line =>  if (line.trim.length==0) "" else line.split(" ")(0)).
     	filter(ip => ip.length()>0).
     	map(ip => (ip,1)).
     	reduceByKey((x,y)=>x+y).
     	sortByKey(true).
     	saveAsTextFile(args(2))

   }
 }