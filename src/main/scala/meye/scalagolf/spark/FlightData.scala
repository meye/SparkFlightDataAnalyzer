package meye.scalagolf.spark

class FlightData(row:String) {
  private val arr = row.replaceAll("NA", "").split(",", -1)

  if (arr.length < 29) {
    println("ERROR_PRINT")
    println(row)
    println(arr.length)
    throw new java.lang.ArrayIndexOutOfBoundsException(arr.length)
  }

  def convert2Int(s:String): Int = s match {
    case "" => 0
    case s => s.toInt
  }

  val year = arr(0) // 1987-2008
  val month = arr(1)  // 1-12
  val	dayofMonth = arr(2) // 1-13
  val	dayOfWeek	 = arr(3) // 1 (Monday) - 7 (Sunday)
  val	depTime	 = arr(4) // actual departure time
  val	crsDepTime = arr(5)  // scheduled departure time (local, hhmm)
  val	arrTime = arr(6)	// actual arrival time (local, hhmm)
  val	crsArrTime = arr(7)	// scheduled arrival time (local, hhmm)
  val	uniqueCarrier = arr(8)	// unique carrier code
  val	flightNum = arr(9)	// flight number
  val tailNum = arr(10)	// plane tail number
  val actualElapsedTime = arr(11)	// in minutes
  val crsElapsedTime = arr(12)	// in minutes
  val airTime = arr(13)	// in minutes
  val arrDelay = convert2Int(arr(14)) // arrival delay, in minutes
  val depDelay = convert2Int(arr(15)) // departure delay, in minutes
  val origin = arr(16)	// origin IATA airport code
  val dest = arr(17)	// destination IATA airport code
  val distance = arr(18)	// in miles
  val taxiIn = arr(19)	// taxi in time, in minutes
  val taxiOut = arr(20)	// taxi out time in minutes
  val cancelled = arr(21)	// was the flight cancelled?
  val cancellationCode = arr(22)	// reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
  val diverted = arr(23)	// 1 = yes, 0 = no
  val carrierDelay = arr(24)	// in minutes
  val weatherDelay = arr(25)	// in minutes
  val nasDelay = arr(26)	// in minutes
  val securityDelay = arr(27)	// in minutes
  val lateAircraftDelay = arr(28)	// in minutes
}
