package sda.parser
import scala.io.Source
object FileReaderUsingIOSource {

  def getContent(file: String): String = {
    println(s"etape 1: $file")
    println(Source.fromFile(file).getLines().mkString)
    Source.fromFile(file).getLines().mkString
  }
}

