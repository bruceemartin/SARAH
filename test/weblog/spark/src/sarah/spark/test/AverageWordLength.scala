package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class CountSum(val cnt: Int=0, val sum: Int=0) extends Serializable {
}

object AverageWordLength {
  
  def sumWithCount(s1:CountSum,s2:CountSum):CountSum =
    new CountSum(s1.cnt+s2.cnt,s1.sum+s2.sum)
  
  def takeaverage(r:(Char,CountSum)) = (r._1,r._2.sum.toDouble/r._2.cnt)
  
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: AverageWordLength <master> <input><output>")
       System.exit(1)
     }
     
     val sc = new SparkContext(args(0),
         "AverageWordLength",
         System.getenv("SPARK_HOME"),
         SparkContext.jarOfClass(this.getClass).toSeq)
     
     sc.textFile(args(1)).
     	flatMap(line => line.split("\\W")).
     	filter(w => w.length()>0).
     	map(w => (w.charAt(0),new CountSum(1,w.length()))).
     	reduceByKey(sumWithCount).
     	sortByKey(true).
     	map(takeaverage).take(100).foreach(println)
     	
   }
}