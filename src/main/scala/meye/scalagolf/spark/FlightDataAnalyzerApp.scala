package meye.scalagolf.spark

import org.apache.spark.{ SparkConf, SparkContext }

object FlightDataAnalyzerApp extends App {

  if (args.length < 3) printUsage()
  else {
    val outfile = args(2) // gs://google-storage-test/flightcount-2008-departure-out/
    val infile = args(1) match {
      case s:String if s.matches("\\d{4}")  => "http://stat-computing.org/dataexpo/2009/" + s + ".csv.bz2"
      case s => s
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val analyzer = new FlightDataAnalyzer(sc.textFile(infile))

    args(0) match {
      case "departure" => {
        analyzer.countDepDelayByYearAndMonth().saveAsTextFile(outfile)
      }
      case "arrival" => {
        analyzer.countArrDelayByYearAndMonth().saveAsTextFile(outfile)
      }
      case _ => {
        printUsage()
      }
    }
  }

  def printUsage(): Unit = {
    println("Usage: program command infile outfile")
    println(" - command : departure or arrival")
  }


}
