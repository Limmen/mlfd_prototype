package kth.se.ii2202.mlfd_prototype.actors

import akka.actor.{Actor, ActorSystem, Props, Timers}
import akka.actor.ActorLogging
import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import java.io.File
import java.text.DecimalFormat
import scala.concurrent.duration.FiniteDuration
import Controller._

/*
 * Controller, this actor will terminate the actorsystem after a timeout
 */
class Controller(system: ActorSystem, timeout: FiniteDuration, simInfo: List[String]) extends Actor with ActorLogging with Timers {

  val nodeDetectionsPath = "data/stats/node_detections.csv"
  val nodeDetectionTimesPath = "data/stats/node_detection_times.csv"
  val nodeFalseSuspicionsPath = "data/stats/node_false_suspicions.csv"
  val nodeCrashCountPath = "data/stats/crashed_nodes.csv"
  val nodeFalseSuspicionCountPath = "data/stats/false_suspicion_count.csv"
  val testInfoPath = "data/stats/test_info.csv"
  val nodeDetectionsFile = new File(nodeDetectionsPath)
  val nodeDetectionTimesFile = new File(nodeDetectionTimesPath)
  val nodeFalseSuspicionsFile = new File(nodeFalseSuspicionsPath)
  val nodeCrashCountFile = new File(nodeCrashCountPath)
  val nodeFalseSuspicionCountFile = new File(nodeFalseSuspicionCountPath)
  val testInfoFile = new File(testInfoPath)

  /*
   * Setup timer for shutting down the system
   */
  override def preStart(): Unit = {
    log.debug("Controller started")
    val nodeDetectionsWriter = CSVWriter.open(nodeDetectionsFile)
    nodeDetectionsWriter.writeRow(List("node", "detectionTimestamp"))
    nodeDetectionsWriter.close()

    val nodeDetectionTimesWriter = CSVWriter.open(nodeDetectionTimesFile)
    nodeDetectionTimesWriter.writeRow(List("node", "detectionTime", "crashTimestamp"))
    nodeDetectionTimesWriter.close()

    val nodeFalseSuspicionsWriter = CSVWriter.open(nodeFalseSuspicionsFile)
    nodeFalseSuspicionsWriter.writeRow(List("node", "timestamp"))
    nodeFalseSuspicionsWriter.close()

    val nodeCrashCountWriter = CSVWriter.open(nodeCrashCountFile)
    nodeCrashCountWriter.writeRow(List("timestamp", "numberOfCrashedNodes"))
    nodeCrashCountWriter.close()

    val falseSuspicionsCountWriter = CSVWriter.open(nodeFalseSuspicionCountFile)
    falseSuspicionsCountWriter.writeRow(List("timestamp", "numberOfFalseSuspicions"))
    falseSuspicionsCountWriter.close()

    /*
    val testInfoWriter = CSVWriter.open(testInfoFile)
    testInfoWriter.writeRow(List("test", "workersCount", "locationsCount", "sampleSize", "defaultMean", "hbTimeout", "stdDevCount", "geoFactor", "crashProb", "delta", "defaultStd", "bandwidthCount", "bandwidthFactor", "batchSize", "learningRate", "regParam", "numIterations", "messageLossProb", "stdevMargin","averageDetectiontime", "mistakeRate"))
    testInfoWriter.close()
     */
    timers.startSingleTimer(ShutDownTimerKey, ShutDownTimeout, timeout)
  }

  /*
   * Receive loop
   * when timeout occurs the actor system is shutdown and the simulation is finished
   */
  def receive = {
    case ShutDownTimeout => {
      log.info(s"Terminating the simulation as the timeout of ${timeout.toSeconds} seconds was reached")
      system.terminate
      log.info("Doing after processing of the data...")
      val suspicionData = CSVReader.open(new File("data/stats/node_suspicions.csv")).all().drop(1)
      val crashData = CSVReader.open(new File("data/stats/node_crashes.csv")).all().drop(1)
      val detections = getDetectionsOfCrashes(suspicionData, crashData)
      val detectionTimes = calculateDetectionTimes(detections, crashData)
      val falseSuspicions = calculateFalseSuspicions(suspicionData, detections, crashData)
      val crashedNodes = crashCount(crashData)
      val falseSuspicionsTime = crashCount(falseSuspicions)
      val averageDetectionTime = calculateAverageDetectionTime(detectionTimes)
      val mistakeRate = calculateMistakeRate(falseSuspicions, timeout.toSeconds.toDouble)
      var writer = CSVWriter.open(nodeDetectionsFile, append = true)
      writer.writeAll(detections)
      writer.close()
      writer = CSVWriter.open(nodeDetectionTimesFile, append = true)
      writer.writeAll(detectionTimes)
      writer.close()
      writer = CSVWriter.open(nodeFalseSuspicionsFile, append = true)
      writer.writeAll(falseSuspicions)
      writer.close()
      writer = CSVWriter.open(nodeCrashCountFile, append = true)
      writer.writeAll(crashedNodes)
      writer.close()
      writer = CSVWriter.open(nodeFalseSuspicionCountFile, append = true)
      writer.writeAll(falseSuspicionsTime)
      writer.close()
      writer = CSVWriter.open(testInfoFile, append = true)
      writer.writeRow(simInfo :+ averageDetectionTime.toString :+ mistakeRate.toString)
      writer.close()

      log.info("After processing finnished")
    }
  }
}

/*
 * Companion object, i.e static fields
 */
object Controller {
  private val formatter = new DecimalFormat("########################.#######################################")
  def props(system: ActorSystem, timeout: FiniteDuration, simInfo: List[String]): Props = {
    Props(new Controller(system, timeout, simInfo))
  }

  /*
   * Unique key for the timer
   */
  case object ShutDownTimerKey

  /*
   * Timer for shutdown of the system
   */
  case object ShutDownTimeout

  def getDetectionsOfCrashes(suspicionData: List[List[String]], crashData: List[List[String]]): List[List[String]] = {
    val crashed: List[List[String]] = suspicionData.filter((line: List[String]) => {
      val node = line(0)
      crashData.foldLeft(false) { (b: Boolean, line: List[String]) =>
        {
          if (line(0) == node)
            true
          else
            b
        }
      }
    })
    val latestSuspicionOnly = crashed.foldLeft(List[List[String]]()) { (acc, line) =>
      {
        val counted = acc.foldLeft(false) { (b, line2) =>
          {
            if (line2(0) == line(0))
              true
            else
              b
          }
        }
        if (counted) {
          acc
        } else {
          val newLine: List[String] = crashed.foldLeft(line) { (l, line2) =>
            {
              if (line2(0) == l(0) && line2(1).toDouble > l(1).toDouble) {
                line2
              } else
                l
            }
          }
          newLine :: acc
        }
      }
    }
    return latestSuspicionOnly
  }

  def calculateDetectionTimes(detections: List[List[String]], crashes: List[List[String]]): List[List[String]] = {
    return detections.map((line: List[String]) => {
      val node = line(0)
      val crashTime = crashes.filter((line2: List[String]) => line2(0) == node)(0)(1)
      val suspicionTime = line(1)
      var detectionTime = suspicionTime.toDouble - crashTime.toDouble
      if (detectionTime < 0)
        detectionTime = 0
      List(node, formatter.format(detectionTime), crashTime)
    })
  }

  def calculateFalseSuspicions(suspicionData: List[List[String]], detections: List[List[String]], crashData: List[List[String]]): List[List[String]] = {
    suspicionData.filter((line: List[String]) => {
      !detections.contains(line)
    })
  }

  def crashCount(crashData: List[List[String]]): List[List[String]] = {
    crashData.map((line: List[String]) => {
      val timestamp = line(1)
      val count: Integer = crashData.foldLeft(0: Integer) { (c, line) =>
        {
          if (line(1).toDouble <= timestamp.toDouble)
            c + 1
          else
            c
        }
      }
      List(timestamp, count.toString)
    })
  }

  def calculateAverageDetectionTime(detectionTimes: List[List[String]]): Double = {
    return detectionTimes.map((line: List[String]) => line(1).toDouble).sum / detectionTimes.length
  }

  def calculateMistakeRate(mistakes: List[List[String]], simulationTime: Double): Double = {
    return mistakes.length / simulationTime
  }
}
