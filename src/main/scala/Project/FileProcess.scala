package Project
 import org.apache.spark.sql.{SparkSession,DataFrame}
 import  Project.config.Settings._

object FileProcess  extends  App{
  implicit val spark= SparkSession.builder().appName("File process")
    .config("spark.master", "local")
    .getOrCreate()
  val dataFrame = spark.read.format("CSV").option("header","true").load(s"$path")

     //dataFrame.show(true)
  val dataRDD = spark.read.csv(s"$path").rdd.take(9).drop(2)
  //dataRDD.take(2).foreach(println)
  //dataRDD.foreach(println)
  val mapFile = dataRDD.map(line => (line,line.length+2))
  mapFile.foreach(println)
 }
