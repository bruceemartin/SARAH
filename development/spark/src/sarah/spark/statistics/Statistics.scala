package sarah.spark.statistics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

  class Statistics  (
      sc: SparkContext, 
      val generateStatistics:Boolean=false, 
      val samplePercentage:Double=1.0F
   ) extends Serializable {
    var numberRecordsInInput = 0L
    var numberRecordsInSample = sc.accumulator(0)
    var numberRecordsInIntermediate = 0L
    
}

object Statistics {
  
  
  implicit class RDDStatistics[V](val rdd: RDD[V]) extends Serializable {    
    var stat:Statistics=null
    
    def sarahRandom(rec:V):Boolean = {
      if (Math.random() < stat.samplePercentage) {
        stat.numberRecordsInSample += 1
        true 
      } else {
        false
      }
    }

    /*
     * Initializes Sarah.  If generate is false, statistics are loaded to be used
     * if true, a random sample is returned
     */
    
    def initialize(stats: Statistics) = {
      stat=stats
      if (stat.generateStatistics) {
        stats.numberRecordsInInput=rdd.count
        rdd.filter(sarahRandom) 
      }
      else rdd
    }
    
  }
  
  implicit class PairStatistics[K,V](val pairRDD: RDD[(K,V)]) extends Serializable {
    /*
     * If generateStatistics is true, generate statistics on random sample
     */
    def computeStatistics(stat:Statistics,fname:String) = {
        stat.numberRecordsInIntermediate = pairRDD.count
        pairRDD 
    }
  }
  
}
  