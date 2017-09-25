package kth.se.ii2202.mlfd_prototype.actors

import java.text.DecimalFormat
import kth.se.ii2202.mlfd_prototype.actors.DataCollector.NodeDied
import org.apache.commons.math3.distribution.{ ExponentialDistribution, NormalDistribution, WeibullDistribution }
import org.apache.commons.math3.random.JDKRandomGenerator
import scala.concurrent.duration._

import Superviser._
import Worker._
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}

/*
 * Worker, this actor will respond to hearbeats with simulated geographic delay and simulated network delay and also will simulate probability of crashes
 */
class Worker(id: Integer, geoLoc: Double, stdDev: Double, geoFactor: Double,
  crashProb: Double, collector: ActorRef, bandwidth: Double, bandwidthFactor: Double,
  messageLossProb: Double, pattern: Boolean, altGeo: Double, altBw: Double,
  bandwidthCount: Integer, geoCount: Integer, distri: Int, warmup: Int)
  extends Actor with ActorLogging with Timers {

  private val formatter = new DecimalFormat("#.#######################################")
  private val random =  new JDKRandomGenerator()
  private var bw: Double = bandwidth
  private var loc: Double = geoLoc
  private var expD : ExponentialDistribution = null
  private var weibullD : WeibullDistribution = null
  private var normalD: NormalDistribution = null
  private var warmupCount = 0

  /*
   * Initialize RTT random distributions
   */
  override def preStart(): Unit = {
    random.setSeed(id)
    log.debug(s"Worker $id, geo-location: $geoLoc, bandwidth: $bandwidth started")
    if (!pattern) {
      bw = altBw
      loc = altGeo
    }
    distri match {
      case 1 => {
        val mean = getGeoDelay + getBandwidthDelay
        expD = new ExponentialDistribution(random, mean)
      }
      case 2 => {
        val mean = getGeoDelay + getBandwidthDelay
        var shape = 1.5
        val scale = stdDev
        weibullD = new WeibullDistribution(random, shape, scale)
      }
      case 3 => {
        val mean = getGeoDelay + getBandwidthDelay
        normalD = new NormalDistribution(random, mean, stdDev)
      }
    }
  }

  /*
   * Receive loop, respond to heartbeats with simulated delays based on
   * randomness and geographic location, also simulate crashes
   */
  def receive = {
    case HeartBeat => {
      log.debug("Worker received heartbeat")
      if(warmupCount >= warmup) {
        if (!messageLoss()) {
        if (!timers.isTimerActive(ReplyDelayTimeoutKey))
          timers.startSingleTimer(ReplyDelayTimeoutKey, ReplyDelayTimeout(sender), getDelay)
        }
      } else {
        warmupCount = warmupCount + 1
      }
    }
    case ReplyDelayTimeout(from) => {
      from ! new HeartBeatReply(id, geoLoc, bandwidth)
      if (crash()) {
        val timeStamp = System.currentTimeMillis().toDouble
        collector ! new NodeDied(List(id.toString(), formatter.format(timeStamp)))
        context.stop(self)
        log.info("worker: " + id + "crashed")
      }
    }
  }

  /*
   * Get simulated delay based on randomness and geo-location
   */
  def getDelay(): FiniteDuration = {
    val geoDelay = getGeoDelay
    val bandwidthDelay = getBandwidthDelay
    return distri match{
      case 1 => expD.sample().millis
      case 2 => {
        val sample = weibullD.sample()
        val truncatedSample = (sample % 0.0000001)
        (geoDelay + bandwidthDelay + truncatedSample).millis
      }
      case 3 => {
        normalD.sample().millis
      }
    }
  }

  /*
   * Calculates delay based on bandwidth
   */
  def getBandwidthDelay() : Double = {
    var bandwidthDelay = 1 * bandwidthFactor
    if (bw > 0)
      bandwidthDelay = (1 / bw * bandwidthFactor)
    return bandwidthDelay
  }

  /*
   * Calculates delay based on geographic location
   */
  def getGeoDelay() : Double = {
    var geoDelay = loc * geoFactor
    return geoDelay
  }

  /*
   * Simulate probability of crash
   */
  def crash(): Boolean = {
    return random.nextDouble() <= crashProb
  }

  /*
   * Simulate probability of message loss
   */
  def messageLoss(): Boolean = {
    return random.nextDouble() <= messageLossProb
  }

  def weibullD(shape : Double, scale: Double): Double = {
    val weibullD : WeibullDistribution = new WeibullDistribution(shape, scale);
    return weibullD.sample()
  }

  def exponentialD(mean : Double): Double = {
    val expD = new ExponentialDistribution(mean)
    return expD.sample()
  }
}

/*
 * Companion object, i.e static fields
 */
object Worker {
  def props(id: Integer, geoLoc: Double, stdDev: Double, geoFactor: Double,
    crashProb: Double, collector: ActorRef, bandwidth: Double, bandwidthFactor: Double,
    messageLossProb: Double, pattern: Boolean, altGeo: Double, altBw: Double,
    bandwidthCount: Integer, geoCount: Integer, distri: Integer, warmup: Integer): Props = {
    Props(new Worker(id, geoLoc, stdDev, geoFactor, crashProb, collector, bandwidth,
      bandwidthFactor, messageLossProb, pattern, altGeo, altBw, bandwidthCount,
    geoCount, distri, warmup))
  }

  /*
   * HeartBeat reply which includes id and location
   */
  final case class HeartBeatReply(from: Integer, loc: Double, bandwidth: Double)

  /*
   * Simulate delay with a radom timer
   */
  case class ReplyDelayTimeout(from: ActorRef)
  case object ReplyDelayTimeoutKey

}
