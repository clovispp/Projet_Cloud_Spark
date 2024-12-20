package sda.parser

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import sda.reader._

object ConfigurationParser {

  implicit val format = DefaultFormats

  def getCsvReaderConfigurationFromJson(jsonString: String): CsvReader = {
    JsonMethods.parse(FileReaderUsingIOSource.getContent(jsonString)).extract[CsvReader]
  }
  def getJsonReaderConfigurationFromJson(jsonString: String): JsonReader = {
    println(JsonMethods.parse(FileReaderUsingIOSource.getContent(jsonString)).extract[JsonReader])
    JsonMethods.parse(FileReaderUsingIOSource.getContent(jsonString)).extract[JsonReader]
  }
  def getXmlReaderConfigurationFromJson(jsonString: String): XmlReader = {
    println(jsonString)
    JsonMethods.parse(FileReaderUsingIOSource.getContent(jsonString)).extract[XmlReader]
  }

  /* Complétez cette fonction. Elle prend un String  en argument et renvoie un objet JsonReader.
     Elle doit être codée de la même manière que la fonction  getCsvReaderConfigurationFromJson

  def getJsonReaderConfigurationFromJson(jsonString: String): JsonReader = {
  ................................................................
  }
   */

}

