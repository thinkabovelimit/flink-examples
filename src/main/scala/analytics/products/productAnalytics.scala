package analytics.products


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.logging.log4j.Logger

import scala.util.control


object productAnalytics {

  def execute(args:Array[String]):Unit={
    /**
     * Write your code here
    * */
    println("sample program")
  }

  def main(args: Array[String]): Unit = {
   try {
     execute(args)
   }
    catch {
      case ex: Exception => ex.formatted("error occured")
    }

  }


}
