package sarah.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object RecordCount {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: RecordCount <input>")
       System.exit(1)
     }

     val sc = new SparkContext()

     val numRecords = sc.textFile(args(0)).count
     println(args(0)+" has "+numRecords+" records.")

   }
 }