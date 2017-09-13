package kth.se.ii2202.mlfd_prototype.actors

import java.text.DecimalFormat
import kth.se.ii2202.mlfd_prototype.actors.DataCollector.NodeDied
import scala.concurrent.duration._

import Superviser._
import Worker._
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}

/*
 * Worker, this actor will respond to hearbeats with simulated geographic delay and simulated network delay and also will simulate probability of crashes
 */
class Worker(id: Integer, geoLoc: Double, stdDev : Double, geoFactor : Double,
  crashProb: Double, collector : ActorRef, bandwidth: Double, bandwidthFactor : Double,
  messageLossProb: Double, pattern: Boolean, altGeo : Double, altBw: Double, rand: Boolean)
    extends Actor with ActorLogging with Timers {

  private val random = new scala.util.Random(id) //Seed with nodeID
  private val formatter = new DecimalFormat("#.#######################################")
  private var bw : Double = bandwidth
  private var loc : Double = geoLoc

  override def preStart(): Unit = {
    log.debug(s"Worker $id, geo-location: $geoLoc, bandwidth: $bandwidth started")
    if(!pattern){
      bw = altBw
      loc = altGeo
    }
  }

  /*
   * Receive loop, respond to heartbeats with simulated delays based on
   * randomness and geographic location, also simulate crashes
   */
  def receive = {
    case HeartBeat => {
      log.debug("Worker received heartbeat")
      if(crash()){
        val timeStamp = System.currentTimeMillis().toDouble
        collector ! new NodeDied(List(id.toString(), formatter.format(timeStamp)))
        context.stop(self)
        log.info("worker: " + id + "crashed")
      }
      else if(!messageLoss()){
        if(!timers.isTimerActive(ReplyDelayTimeoutKey))
          timers.startSingleTimer(ReplyDelayTimeoutKey, ReplyDelayTimeout(sender), getDelay)
      }
    }
    case ReplyDelayTimeout(from) => {
        from ! new HeartBeatReply(id, geoLoc, bandwidth)
    }
  }

  /*
   * Get simulatd delay based on randomness and geo-location
   */
  def getDelay(): FiniteDuration = {
    var geoDelay = (loc * loc * loc) * geoFactor
    var bandwidthDelay = 1 * bandwidthFactor
      if(bw > 0)
        bandwidthDelay = ((1/(bw * bw * bw)) * bandwidthFactor)

    if(rand){
      val gl = random.nextInt(30)
      geoDelay = ((gl * gl * gl) * random.nextInt(10)).toDouble
      val bf = random.nextInt(10000)
      val b = random.nextInt(30)
      bandwidthDelay = (1 * bf).toDouble
      if(b > 0)
        bandwidthDelay = ((1/(b*b*b)) * bf).toDouble
    }
    return ((random.nextGaussian()*stdDev) + geoDelay + bandwidthDelay).millis
  }

  def crash() : Boolean = {
    return random.nextDouble() <= crashProb
  }

  def messageLoss( ): Boolean = {
    return random.nextDouble() <= messageLossProb
  }
}

/*
 * Companion object, i.e static fields
 */
object Worker {
  def props(id: Integer, geoLoc: Double, stdDev: Double, geoFactor: Double,
    crashProb : Double, collector: ActorRef, bandwidth: Double, bandwidthFactor: Double,
    messageLossProb: Double, pattern: Boolean, altGeo : Double, altBw: Double, rand : Boolean): Props = {
    Props(new Worker(id, geoLoc, stdDev, geoFactor, crashProb, collector, bandwidth, bandwidthFactor, messageLossProb, pattern, altGeo, altBw, rand))
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
