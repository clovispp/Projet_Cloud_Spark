package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import java.time.LocalDate


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

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
      val date_regex_expression = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      val new_dataframe = dataFrame
        .withColumn("MetaData", from_json(col("MetaData"), schema))
        .withColumn("Ville", col("MetaData.MetaTransaction").getItem(0).getField("Ville"))
        .withColumn("Date_End_contrat", col("MetaData.MetaTransaction").getItem(0).getField("Date_End_contrat"))
        .withColumn("Date_End_contrat", regexp_extract(col("Date_End_contrat"), date_regex_expression, 0))
        .drop("MetaData")
      new_dataframe
    }

    def contratStatus(): DataFrame = {
      val currentDate = LocalDate.now().toString
      val contrat_status = dataFrame
        .withColumn("Contrat_Status", when(col("Date_End_contrat") < lit(currentDate), "Expired").otherwise("Active"))

      contrat_status
    }


  }

}