import Project.MetadataManager
import org.apache.spark.sql.{DataFrame,SparkSession}

class Greeter(prefix: String, suffix: String) {
  def greet(name: String): Unit =
    println(prefix + name + suffix)
}
object Main extends App {
  object math {def square(x: Int)=x*x}
  implicit val spark= SparkSession.builder().appName("Good Program")
  .config("spark.master", "local")
  .getOrCreate()
  run()
  def run() = MetadataManager.reportStatus("Hey i am sending you a variable") {
    def name: String = System.getProperty("user.name")

    println("inside the main def"+name)
     val reportTime = System.currentTimeMillis()
    println(reportTime)
    var states = scala.collection.mutable.Map("AL" -> "Alabama", "AK" -> "Alaska")
    for ((k,v) <- states) printf("key: %s, value: %s\n", k, v)
    states foreach (x => println (x._1 + "-->" + x._2))
    val greeter = new Greeter("Hello, ", "!")
    greeter.greet("Scala developer")
    println(math square 6)
  }
}