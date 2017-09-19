package kth.se.ii2202.mlfd_prototype.actors

import akka.actor.{Actor, ActorLogging, Props}
import com.github.tototoshi.csv.CSVWriter
import java.io.File
import DataCollector._

/*
 * Actor that coordinates writing statistics of the simulation to .csv files
 */
class DataCollector() extends Actor with ActorLogging {

  val nodeCrashesPath = "data/stats/node_crashes.csv"
  val suspectedNodesPath = "data/stats/suspected_nodes.csv"
  val rttDataPath = "data/stats/rtt_data.csv"
  val predictionDataPath = "data/stats/prediction_data.csv"
  val nodeSuspicionsPath = "data/stats/node_suspicions.csv"
  val testInfoPath = "data/stats/test_info.csv"
  val bandwidthDelayPath = "data/stats/bandwidth_delay.csv"
  val geoDelayPath = "data/stats/geo_delay.csv"
  val stdDevPath = "data/stats/stddev.csv"

  /*
   * Overwrite old files in data/stats/ with csv headers
   */
  override def preStart(): Unit = {
    val nodeCrashFile = new File(nodeCrashesPath)
    val suspectedNodesFile = new File(suspectedNodesPath)
    val rttDataFile = new File(rttDataPath)
    val predictionDataFile = new File(predictionDataPath)
    val nodeSuspicionsFile = new File(nodeSuspicionsPath)
    val testInfoFile = new File(testInfoPath)
    val bandwidthDelayFile = new File(bandwidthDelayPath)
    val geoDelayFile = new File(geoDelayPath)
    val stdDevFile = new File(stdDevPath)

    val nodeCrashWriter = CSVWriter.open(nodeCrashFile)
    nodeCrashWriter.writeRow(List("node", "timestamp"))
    nodeCrashWriter.close()

    val suspectedNodesWriter = CSVWriter.open(suspectedNodesFile)
    suspectedNodesWriter.writeRow(List("timestamp", "numberOfSuspectedNodes"))
    suspectedNodesWriter.close()

    val rttDataWriter = CSVWriter.open(rttDataFile)
    rttDataWriter.writeRow(List("node", "rttMean", "rttStd", "geo", "rttMin", "rttMax", "bandwidth", "rtt", "timestamp"))
    rttDataWriter.close()

    val predictionDataWriter = CSVWriter.open(predictionDataFile)
    predictionDataWriter.writeRow(List("node", "timestamp", "prediction"))
    predictionDataWriter.close()

    val nodeSuspicionsWriter = CSVWriter.open(nodeSuspicionsFile)
    nodeSuspicionsWriter.writeRow(List("node", "suspected"))
    nodeSuspicionsWriter.close()

    val bandwidthDelayWriter = CSVWriter.open(bandwidthDelayFile)
    bandwidthDelayWriter.writeRow(List("bandwidth", "delay"))
    bandwidthDelayWriter.close()

    val geoDelayWriter = CSVWriter.open(geoDelayFile)
    geoDelayWriter.writeRow(List("location", "delay"))
    geoDelayWriter.close()

    val stddevWriter = CSVWriter.open(stdDevFile)
    stddevWriter.writeRow(List("stddev"))
    stddevWriter.close()
  }

  /*
   * receive-loop, receives different type of data and write to different .csv files
   */
  def receive = {
    case NodeDied(row) => writePoint(nodeCrashesPath, row)
    case NumberOfSuspectedNode(row) => writePoint(suspectedNodesPath, row)
    case RTTData(row) => writePoint(rttDataPath, row)
    case Prediction(row) => writePoint(predictionDataPath, row)
    case Suspicion(row) => writePoint(nodeSuspicionsPath, row)
    case BandwidthDelay(row) => writePoint(bandwidthDelayPath, row)
    case GeoDelay(row) => writePoint(geoDelayPath, row)
    case Stdev(row) => writePoint(stdDevPath, row)
  }

  /*
   * Write a row of data to specified csv file
   */
  def writePoint(path: String, row: List[String]): Unit = {
    val file = new File(path)
    val writer = CSVWriter.open(file, append = true)
    writer.writeRow(row)
    writer.close()
  }
}

/*
 * Companion object, i.e static fields
 */
object DataCollector {
  def props(): Props = {
    Props(new DataCollector())
  }

  /*
   * Sent by workers before they crash
   */
  case class NodeDied(row: List[String])
  /*
   * Sent by supervisers for each timeout
   */
  case class NumberOfSuspectedNode(row: List[String])
  /*
   * Sent by MLFD Failuredetector for each RTT data that is collected and fed into
   * the ML model
   */
  case class RTTData(row: List[String])
  /*
   * Sent by MLFD Failuredetector when making a timeout-prediciton to decide if a node
   * is dead or not
   */
  case class Prediction(row: List[String])
  /*
   * Sent by EPFD, MLFD when a node is suspected to have crashed
   */
  case class Suspicion(row: List[String])

  case class BandwidthDelay(row: List[String])
  case class GeoDelay(row: List[String])
  case class Stdev(row: List[String])
}
