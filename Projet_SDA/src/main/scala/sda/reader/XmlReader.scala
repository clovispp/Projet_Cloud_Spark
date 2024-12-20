package sda.reader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
case class XmlReader(path: String
                     )
  extends Reader {
  val format = "xml"
  val myschama = new StructType()
  def read()(implicit  spark: SparkSession): DataFrame = {

    spark.read.format(format)
      .option("rowTag", "Client")
      .load(path)

  }
}