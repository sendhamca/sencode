package Project

import Project.config.Settings._
import scala.util.{Failure,Success,Try}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Logger extends App {
  implicit val spark= SparkSession.builder().appName("Good Program")
    .config("spark.master", "local")
    .getOrCreate()

  val schema = StructType(
    Array(
      StructField("File_Date",StringType,false),
      StructField("Crt_Time",StringType,false),
      StructField("Source_Name",StringType,false) ,
      StructField("Site_Name", StringType, false),
      StructField("File_Name", StringType, false),
      StructField("FileRec_cnt", IntegerType, false),
      StructField("File_Path", StringType, false),
      StructField("bad_record", StringType, false)
    )
  )
  run(createDFfromCSV)

  def run(dataFrame: DataFrame)=InsertMapRDB(dataFrame){
    isDatasetEmpty(dataFrame)
  }

  def isDatasetEmpty(dataFrame: DataFrame): Boolean =
  {
    Try{dataFrame.first.length != 0} match {
      case Success(_) => false
      case Failure(_) => true }
    }

private def createDFfromCSV(): DataFrame= {
    spark
    .read
    .format("CSV")
    .option("header","false")
    .schema(schema).option("delimiter",",")
    .option("columnNameOfCorruptRecord", "bad_record")
    .load(s"$log_path")
    .toDF()
  }

  def InsertMapRDB(dataFrame: DataFrame)(callback: => Unit)(implicit spark: SparkSession)={
    print("Insert to dB Started")
    val toIns= dataFrame.where(dataFrame.col("bad_record").isNull)

    Try(callback) match {
      case Success(v) => postRun(success = true,toIns)
      case Failure(e) => postRun(success = false,toIns) ; throw e
    }
  }
  def postRun(success: Boolean,dataFrame: DataFrame): Unit =
  {
    print("MapR DB started")
    dataFrame.show()

  }

}