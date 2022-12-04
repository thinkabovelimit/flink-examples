package analytics.products

import analytics.products.productAnalytics.execute
import org.json4s.{DefaultFormats, Formats}

import java.time.LocalDateTime
import scala.util.Random
import java.io.FileWriter
import java.io.File
import java.time.Instant
import org.json4s.jackson.Serialization.read

import scala.io.Source.fromFile

/**
 * Scala code to generate data stream in every five minutes
 *
 * */
object DataStreamGenerator extends Thread {

  case class JobConfig(filePath: String, streamTime: Long, numberOfFiles: Long, recordsPerFile: Long)


  def generateFile(location: String, fileName: String, value: Long): Unit = {
    val filePath = location + "/" + fileName
    val f = new File(filePath)
    f.createNewFile()
    val writer = new FileWriter(filePath)
    var writeString = "{},"
    for (j <- Range.inclusive(0, value.toInt, 1)) {
      println(j)
      val productName = "productName" + j
      val timeOfSale = Instant.now().toEpochMilli
      val amount = Random.nextDouble()
      val quantitySold = Random.nextInt(1000)
      if (j != value) {
        writeString = s"""{"productName":"${productName}","timeOfSale":${timeOfSale},"amount":${amount},"quantitySold":${quantitySold}},"""
      }
      else {
        writeString = s"""{"productName":"${productName}","timeOfSale":${timeOfSale},"amount":${amount},"quantitySold":${quantitySold}}"""
      }
      writer.append(writeString)
      writer.append('\n')
    }
    writer.close()


  }


  override def run(): Unit = {
    implicit lazy val serializerFormats: Formats = DefaultFormats
    val path = getClass.getResource("/DataStreamJobproperties.json").getPath
    val bufferedFile = fromFile(path)
    val config = read[JobConfig](bufferedFile.mkString)
    bufferedFile.close
    var i = 0
    val location = config.filePath
    while (i <= config.numberOfFiles) {
      val fileName = "out" + i.toString + ".json"
      try {
        generateFile(location, fileName, config.recordsPerFile)
      }
      catch {
        case ex: Exception => println(ex)
      }
      try {
        Thread.sleep(config.streamTime)
      }
      catch {
        case ex: Exception => println(ex)
      }
      i = i + 1
    }
  }

  def main(args: Array[String]): Unit = {
    DataStreamGenerator.start()
  }

}
