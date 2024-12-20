package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {
    val date_regex_expression = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
    def formatter(): DataFrame = {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC () : DataFrame ={

      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
        .withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast("double"))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))
        .withColumn("TTC", round(col("HTT")+ (col("TVA")*col("HTT")),2))
        .drop("TVA","HTT")
    }

    def extractDateEndContratVille(): DataFrame = {
      val path = "C:\\Users\\daora\\IdeaProjects\\Projet_SDA\\src\\main\\resources\\Configuration\\reader_json.json"
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)
      dataFrame
    }

    /*def contratStatus(): DataFrame = {
      /*..........................coder ici...............................*/
    }*/


  }

}