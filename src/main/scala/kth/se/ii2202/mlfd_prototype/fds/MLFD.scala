package kth.se.ii2202.mlfd_prototype.fds

import java.text.DecimalFormat
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import kth.se.ii2202.mlfd_prototype.actors.Superviser._
import kth.se.ii2202.mlfd_prototype.actors.DataCollector._
import kth.se.ii2202.mlfd_prototype.actors.Worker.HeartBeatReply
import kth.se.ii2202.mlfd_prototype.ml.MLFDModel
import org.apache.log4j.LogManager
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/*
 * Failure detector that uses a machine learning model to predict failures
 */
class MLFD(workers: List[WorkerEntry], sampleWindowSize: Integer, defaultMean: Double,
  collector: ActorRef, defaultStd: Double,
  batchSize: Integer, learningRate: Double, regParam: Double, numIterations: Integer,
  stdevMargin: Double) extends FD {

  private val log = LogManager.getRootLogger
  private var all: Set[WorkerEntry] = Set()
  private var alive: Set[WorkerEntry] = Set()
  private var suspected: Set[WorkerEntry] = Set()
  private val mlfdModel = new MLFDModel(batchSize = batchSize, learningRate = learningRate, regParam = regParam, numIterations = numIterations)
  private var outStandingHBs: Map[Integer, Double] = Map()
  private var responseData: Map[Integer, List[Double]] = Map()
  private val formatter = new DecimalFormat("#.#######################################")

  init(workers)

  /*
   * Initialize FD state
   */
  def init(workers: List[WorkerEntry]): Unit = {
    alive = alive ++ workers
    all = all ++ workers
    workers.map((worker: WorkerEntry) => responseData += (worker.workerId -> List()))
  }

  /*
   * Handle  timeout, i.e analyze based on received heartbeats and ML-model which nodes should be considered dead
   */
  def timeout(): Set[WorkerEntry] = {
    val timeStamp = System.currentTimeMillis().toDouble
    log.info("Suspected nodes: " + suspected.size + " alive: " + alive.size)
    collector ! new NumberOfSuspectedNode(List(formatter.format(timeStamp), suspected.size.toString))
    all.map((worker: WorkerEntry) => {
      val mean = getMeanRespTime(worker.workerId)
      val min = getMinRespTime(worker.workerId)
      val max = getMaxRespTime(worker.workerId)
      val sdev = stdDev(getVariance(worker.workerId))
      val predictedTimeout = mlfdModel.predict(mean, sdev, worker.loc, min, max, worker.bandwidth)
      collector ! new Prediction(List(worker.workerId.toString, formatter.format(timeStamp), predictedTimeout.toString))
      if (!alive.contains(worker) & !suspected.contains(worker)) {
        getTimeSinceHb(worker.workerId) match {
          case Some(currentTimeout) => {
            val len = responseData(worker.workerId).length
            val workerId = worker.workerId
            if ((predictedTimeout + sdev * stdevMargin) < currentTimeout) {
              val workerId = worker.workerId
              log.info(s"worker $workerId dead, predict: $predictedTimeout, current timeout: $currentTimeout")
              suspected = suspected + worker
              collector ! new Suspicion(List(worker.workerId.toString, formatter.format(timeStamp)))
            }
          }
          case None => ;
        }
      } else if (alive.contains(worker) && suspected.contains(worker)) {
        log.debug("Detected premature crash of worker: " + worker.workerId)
        suspected = suspected - worker
      }
    })
    all.map((worker: WorkerEntry) => {
      if (alive.contains(worker))
        outStandingHBs = outStandingHBs + (worker.workerId -> timeStamp)
    })
    alive = Set()
    return all
  }

  /*
   * Received reply from worker, i.e update state and the ML-model
   */
  def receivedReply(hbReply: HeartBeatReply, sender: ActorRef): Unit = {
    val timeStamp = System.currentTimeMillis().toDouble
    getTimeSinceHb(hbReply.from) match {
      case Some(responseTime) => {
        addRespTime(hbReply.from, responseTime)
        val mean = getMeanRespTime(hbReply.from)
        val min = getMinRespTime(hbReply.from)
        val max = getMaxRespTime(hbReply.from)
        val sdev = stdDev(getVariance(hbReply.from))
        val dataPoint = LabeledPoint(responseTime.toDouble, Vectors.dense(mean, sdev, hbReply.loc.toDouble, min, max, hbReply.bandwidth))
        collector ! new RTTData(List(hbReply.from.toString(), mean.toString(), sdev.toString(), hbReply.loc.toString, min.toString, max.toString(), hbReply.bandwidth.toString(), responseTime.toString, formatter.format(timeStamp)))
        mlfdModel.addDataPoint(dataPoint)
      }
      case None => ; //Delayed response to duplicate-hb request, this is OK
    }
    searchSetByRef(all, sender) match {
      case Some(worker) => {
        alive = alive + worker
      }
      case None => log.error("Received heartbeat from worker not supervised")
    }
  }

  /*
   * Helper function to compute millisecond difference since heartbeat-request was sent to worker
   */
  def getTimeSinceHb(workerId: Integer): Option[Double] = {
    if (outStandingHBs.keySet.exists(_ == workerId)) {
      val currentTime = System.currentTimeMillis().toDouble
      val reqTime = outStandingHBs(workerId)
      return Some(currentTime - reqTime)
    } else
      return None
  }

  /*
   * Helper function to add a rtt-sample for a given worker
   */
  def addRespTime(workerId: Integer, responseTime: Double): Unit = {
    var rttList = responseData(workerId)
    rttList = rttList.zipWithIndex.collect {
      case (x, i) if i < sampleWindowSize - 2 => x
    }
    rttList = responseTime :: rttList
    responseData += (workerId -> rttList)
  }

  /*
   * Helper function to calculate mean-RTT for a worker based on samples
   */
  def getMeanRespTime(workerId: Integer): Double = {
    if (responseData(workerId).length == 0)
      return defaultMean
    else
      return responseData(workerId).sum / responseData(workerId).length
  }

  /*
   * Helper function to calculate variance of RTTs for a worker based on samples
   */
  def getVariance(workerId: Integer): Double = {
    if (responseData(workerId).length < 2)
      return Math.pow(defaultStd, 2)
    val mean = getMeanRespTime(workerId)
    val variances = responseData(workerId).map((respTime: Double) => Math.pow((respTime - mean), 2))
    val variance = variances.sum / variances.length
    return variance
  }

  /*
   * Helper function to calculate STD based on variance
   */
  def stdDev(variance: Double): Double = {
    Math.sqrt(variance)
  }

  /*
   * Helper function to get min resp time from samples
   */
  def getMinRespTime(workerId: Integer): Double = {
    if (responseData(workerId).length == 0)
      return defaultMean
    else
      return responseData(workerId).min
  }

  /*
   * Helper function to get max resp time from samples
   */
  def getMaxRespTime(workerId: Integer): Double = {
    if (responseData(workerId).length == 0)
      return defaultMean
    else
      return responseData(workerId).max
  }

  def workers() : Set[WorkerEntry] = {
    return workers.toSet
  }
}
