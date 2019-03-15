package Project
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
object MetadataManager {

  def reportStatus(keyPrefix: String)(callback: => Unit)(implicit spark: SparkSession)={
    println("Started "+keyPrefix)

    try{
      callback
    }
    catch{
      case e: Exception => println("exception caught: " + e);
      case e: IllegalArgumentException => println("illegal arg. exception");
      case e: IllegalStateException    => println("illegal state exception");
    }
    //Seq[String] = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(new Path(dir))

  }

}
