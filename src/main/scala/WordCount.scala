import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount  {
  def main (arg: Array[String])  {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://mycluster/user/hadoop/conf/hadoop-env.sh")
//    val data = sc.textFile("/Users/grigri/workspace/StreamDataAnalysis/SparkApp/pom.xml")
    val counts = data.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("<<==========line counts: =========>>: " + counts.count)
    
    sc.stop
  }
}