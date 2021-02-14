
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object ChicagoCrimeFlink extends App{

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // Define the data source
  val rides: DataStream[String] = env
    .readTextFile(args(0))
  case class ChicagoCrimeData(ID: String,
                              Date: String,
                              primaryType: String,
                              locationDescription: String,
                              arrest: String,
                              domestic: String,
                              year: String
                     )
  val splittedDataset= rides
    .map{ crime => crime.split(",")}
    .map{ crime => ChicagoCrimeData(crime(1),crime(3),crime(6),crime(8),crime(9),crime(10),crime(18))}
    .keyBy(_.ID)

  splittedDataset.writeAsCsv(args(1))
    .name("ConvertToCsvFile")
  splittedDataset.print()
    .name("Print Data")
  env.execute("Printing Chicago Crime Data from CSV")
}
