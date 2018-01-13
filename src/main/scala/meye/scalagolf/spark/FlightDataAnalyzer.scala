package meye.scalagolf.spark

class FlightDataAnalyzer(val rdd:org.apache.spark.rdd.RDD[String]) {

  private def loadData():org.apache.spark.rdd.RDD[FlightData] = {
    rdd.mapPartitionsWithIndex((partitionIdx: Int, lines: Iterator[String]) => {
        if (partitionIdx == 0) lines.drop(1)
        lines
      }).map(new FlightData(_))
  }

  def countDepDelayByYearAndMonth():org.apache.spark.rdd.RDD[String] = {
    loadData()
      .filter(_.depDelay > 0)
      .map(data => ((data.year, data.month), 1))
      .reduceByKey((x, y) => x + y)
      .map(row => s"${row._1._1},${row._1._2} ${row._2}")
  }

  def countArrDelayByYearAndMonth():org.apache.spark.rdd.RDD[String] = {
    loadData()
      .filter(_.arrDelay > 0)
      .map(data => ((data.year, data.month), 1))
      .reduceByKey((x, y) => x + y)
      .map(row => s"${row._1._1},${row._1._2} ${row._2}")
  }
}
