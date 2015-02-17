package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Sort {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: Sort <input> <output>")
       System.exit(1)
     }

     val sc = new SparkContext()

     val inputRDD = sc.textFile(args(0))
     inputRDD.map(line=>(line,"")).sortByKey(true, 1).saveAsTextFile(args(1))

   }
 }