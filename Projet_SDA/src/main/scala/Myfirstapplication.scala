import org.apache.spark.sql.SparkSession


object Myfirstapplication{

  def main(args: Array[String]) {

    val csvFile="src/main/resources/data.json"

    val spark = SparkSession.builder.appName("SimpleApplication")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .load(csvFile)

    df.show()

  }
}