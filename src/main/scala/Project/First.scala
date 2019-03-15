package Project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object First  extends  App {
  println("Started")
  val conf= new SparkConf().setAppName("MyFirstSpark").setMaster("local")
    val sc = new SparkContext(conf)
  val data = sc.textFile("C:\\\\Senthil\\\\SenStudy\\\\Scala\\\\Files\\\\data.txt")
  val splitRdd = data.map( line => line.split(",")).saveAsTextFile("C:\\\\output.txt")

  // printing values
  //splitRdd.foreach { x => x.foreach { y => println(y) } }
  println("Completed")
}
