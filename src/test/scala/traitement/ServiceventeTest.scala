package sda.traitement

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import sda.traitement.ServiceVente.DataFrameUtils

class ServiceVenteTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("ServiceVente Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Données fictives pour les tests
  val testData = Seq(
    (1, "100,5|0,19", "{\"MetaTransaction\":[{\"Ville\":\"Paris\",\"Date_End_contrat\":\"2024-12-23\"}]}"),
    (2, "120,546|0,20", "{\"MetaTransaction\":[{\"Ville\":\"Alger\",\"Date_End_contrat\":\"2020-12-23\"}]}"),
    (3, "5,546|0,15", "{\"MetaTransaction\":[{\"Ville\":\"Dakar\",\"Date_End_contrat\":\"2023-12-23\"}]}")
  ).toDF("Id_Client", "HTT_TVA", "MetaData")

  test("formatter should split HTT_TVA into HTT and TVA") {
    val result = testData.formatter()

    val expected = Seq(
      (1, "100,5|0,19", "100,5", "0,19"),
      (2, "120,546|0,20", "120,546", "0,20"),
      (3, "5,546|0,15", "5,546", "0,15")
    ).toDF("Id_Client", "HTT_TVA", "HTT", "TVA")

    assert(result.select("HTT", "TVA").collect() === expected.select("HTT", "TVA").collect())
  }

  test("calculTTC should compute TTC correctly") {
    val result = testData.calculTTC()

    val expected = Seq(
      (1, 119.6),
      (2, 144.66),
      (3, 6.38)
    ).toDF("Id_Client", "TTC")

    assert(result.select("Id_Client", "TTC").collect() === expected.collect())
  }

  test("extractDateEndContratVille should extract Ville and Date_End_contrat") {
    val result = testData.extractDateEndContratVille()

    val expected = Seq(
      (1, "Paris", "2024-12-23"),
      (2, "Alger", "2020-12-23"),
      (3, "Dakar", "2023-12-23")
    ).toDF("Id_Client", "Ville", "Date_End_contrat")

    assert(result.select("Id_Client", "Ville", "Date_End_contrat").collect() === expected.collect())
  }

  test("contratStatus should correctly identify expired contracts") {
    val result = testData
      .extractDateEndContratVille()
      .contratStatus()

    val currentDate = java.time.LocalDate.now().toString
    val expectedStatus = Seq(
      (1, "Paris", "2024-12-23", "Active"),
      (2, "Alger", "2020-12-23", "Expire"),
      (3, "Dakar", "2023-12-23", if (currentDate > "2023-12-23") "Expire" else "Active")
    ).toDF("Id_Client", "Ville", "Date_End_contrat", "Contrat_Status")

    assert(result.select("Id_Client", "Ville", "Date_End_contrat", "Contrat_Status").collect() === expectedStatus.collect())
  }
}
