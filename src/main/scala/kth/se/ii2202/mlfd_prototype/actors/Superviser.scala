package kth.se.ii2202.mlfd_prototype.actors

import scala.concurrent.duration.FiniteDuration

import Superviser._
import Worker._
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import kth.se.ii2202.mlfd_prototype.fds._

/*
 * Superviser, this actor will monitor a set of workers with a failure detector
 */
class Superviser(fd : FD, timeout: FiniteDuration) extends Actor with ActorLogging with Timers {


  /*
   * Setup timer to send heartbeats to all workers every timeout
   */
  override def preStart(): Unit = {
    log.debug("Superviser started")
    timers.startSingleTimer(FDTimerKey, FDTimeout, timeout)
    log.debug("Timer set to: " + timeout.toSeconds + " seconds")
  }

  /*
   * Receive loop
   * when timeout occurs, let the failuredetector update its status about alive/dead nodes
   * and then ping every worker with a new heartbeat.
   * When a heartbeat-reply is received, let the failure detector record this and update its state
   */
  def receive = {
    case FDTimeout => {
      val workers = fd.timeout()
      workers.map((worker : WorkerEntry) => worker.actorRef ! HeartBeat)
      timers.startSingleTimer(FDTimerKey, FDTimeout, timeout)
    }
    case hbReply : HeartBeatReply => {
      fd.receivedReply(hbReply, sender)
    }
  }
}

/*
 * Companion object, i.e static fields
 */
object Superviser {
  def props(fd: FD, timeout: FiniteDuration): Props = {
    Props(new Superviser(fd, timeout))
  }

  /*
   * A worker is represented by unique actor-ref, unique id, and a geographic location
   */
  case class WorkerEntry(actorRef: ActorRef, workerId: Integer, loc: Double, bandwidth: Double)
  /*
   * HeartBeat message
   */
  case object HeartBeat
  /*
   * Timer for failure detection
   */
  case object FDTimeout
  /*
   * Unique key for the timer
   */
  case object FDTimerKey
}
