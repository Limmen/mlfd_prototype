package kth.se.ii2202.mlfd_prototype.actors

import akka.actor.{Actor, ActorLogging, Props}
import com.github.tototoshi.csv.CSVWriter
import java.io.File
import DataCollector._

class DataCollector() extends Actor with ActorLogging {

  val nodeCrashesPath = "data/stats/node_crashes.csv"
  val suspectedNodesPath = "data/stats/suspected_nodes.csv"
  val rttDataPath = "data/stats/rtt_data.csv"
  val predictionDataPath = "data/stats/prediction_data.csv"
  val nodeSuspicionsPath = "data/stats/node_suspicions.csv"
  val testInfoPath = "data/stats/test_info.csv"

  override def preStart(): Unit = {
    val nodeCrashFile= new File(nodeCrashesPath)
    val suspectedNodesFile= new File(suspectedNodesPath)
    val rttDataFile= new File(rttDataPath)
    val predictionDataFile= new File(predictionDataPath)
    val nodeSuspicionsFile = new File(nodeSuspicionsPath)
    val testInfoFile = new File(testInfoPath)

    val nodeCrashWriter = CSVWriter.open(nodeCrashFile)
    nodeCrashWriter.writeRow(List("node", "timestamp"))
    nodeCrashWriter.close()

    val suspectedNodesWriter = CSVWriter.open(suspectedNodesFile)
    suspectedNodesWriter.writeRow(List("timestamp", "numberOfSuspectedNodes"))
    suspectedNodesWriter.close()

    val rttDataWriter = CSVWriter.open(rttDataFile)
    rttDataWriter.writeRow(List("node", "geoLocation", "rtt", "timestamp", "mean"))
    rttDataWriter.close()

    val predictionDataWriter = CSVWriter.open(predictionDataFile)
    predictionDataWriter.writeRow(List("node", "timestamp", "prediction"))
    predictionDataWriter.close()

    val nodeSuspicionsWriter = CSVWriter.open(nodeSuspicionsFile)
    nodeSuspicionsWriter.writeRow(List("node", "suspected"))
    nodeSuspicionsWriter.close()

    val testInfoWriter = CSVWriter.open(testInfoFile)
    testInfoWriter.writeRow(List("test", "workersCount", "locationsCount", "sampleSize", "defaultMean", "hbTimeout", "randomMillis", "geoFactor", "crashProb", "delta"))
    testInfoWriter.close()
  }

  def receive = {
    case NodeDied(row) => writePoint(nodeCrashesPath, row)
    case NumberOfSuspectedNode(row) => writePoint(suspectedNodesPath, row)
    case RTTData(row) => writePoint(rttDataPath, row)
    case Prediction(row) => writePoint(predictionDataPath, row)
    case Suspicion(row) => writePoint(nodeSuspicionsPath, row)
    case SimulationInfo(row) => writePoint(testInfoPath, row)
  }

  def writePoint(path : String, row : List[String]) : Unit = {
    val file = new File(path)
    val writer = CSVWriter.open(file, append=true)
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

  case class NodeDied (row: List[String])
  case class NumberOfSuspectedNode (row: List[String])
  case class RTTData (row: List[String])
  case class Prediction (row : List[String])
  case class Suspicion (row : List[String])
  case class SimulationInfo (row : List[String])
}
