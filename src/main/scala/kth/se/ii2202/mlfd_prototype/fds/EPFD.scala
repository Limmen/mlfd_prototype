package kth.se.ii2202.mlfd_prototype.fds

import java.text.DecimalFormat
import scala.concurrent.duration._

import akka.actor.ActorRef
import kth.se.ii2202.mlfd_prototype.actors.Superviser._
import kth.se.ii2202.mlfd_prototype.actors.Worker.HeartBeatReply
import kth.se.ii2202.mlfd_prototype.actors.DataCollector._

/*
 * Eventual Perfect Failure Detector, implemented from pseudo-code given in the book:

 * Introduction to Reliable and Secure Distributed Programming
 * Authors:
 * - Christian Cachin
 * - Rachid Guerraoui
 * - Lus Rodrigues
 *
 */
class EPFD(workers: List[WorkerEntry], delta: FiniteDuration, collector: ActorRef, timeout: FiniteDuration) extends FD {

  private var all: Set[WorkerEntry] = Set()
  private var alive: Set[WorkerEntry] = Set()
  private var suspected: Set[WorkerEntry] = Set()
  private var delay: FiniteDuration = timeout
  private val formatter = new DecimalFormat("#.#######################################")
  private var responseData: Map[Integer, Double] = Map()
  init(workers)

  /*
   * Initialize the FD state
   */
  def init(workers: List[WorkerEntry]): Unit = {
    println("EPFD started")
    alive = alive ++ workers
    all = all ++ workers
    val timestamp = System.currentTimeMillis().toDouble
    workers.map((worker: WorkerEntry) => responseData += (worker.workerId -> timestamp))
  }

  /*
   * Handle timeout, check if all alive nodes have responded, otherwise suspect them.
   * If a suspected node responded, restore it and increase timeout
   */
  def timeout(): Set[WorkerEntry] = {
    val timeStamp = System.currentTimeMillis().toDouble
    collector ! new NumberOfSuspectedNode(List(formatter.format(timeStamp), suspected.size.toString))
    workers.map((worker: WorkerEntry) => {
      if ((timeStamp - responseData(worker.workerId)) < delay.toMillis.toDouble)
        alive = alive + worker
    })
    println("Suspected nodes: " + suspected.size)
    if ((alive & suspected).size != 0) {
      println("Detected premature crash of : " + (alive & suspected).size + " nodes, increasing timeout with delta: " + delta)
      delay = (delay.toMillis + delta.toMillis).millis
      println("New timeout value is: " + delay.toSeconds + " seconds")
    }
    all.map((worker: WorkerEntry) => {
      if (!alive.contains(worker) && !suspected.contains(worker)) {
        println("Detected crash of node: " + worker.workerId)
        collector ! new Suspicion(List(worker.workerId.toString, formatter.format(timeStamp)))
        suspected = suspected + worker
      } else if (alive.contains(worker) && suspected.contains(worker)) {
        //println("Got reply from a suspected node, : " + worker.workerId + ", restoring it")
        suspected = suspected - worker
      }
    })
    alive = Set()
    return all
  }

  /*
   * Received reply from worker, record this in the FD state.
   */
  def receivedReply(hbReply: HeartBeatReply, sender: ActorRef): Unit = {
    val timeStamp = System.currentTimeMillis().toDouble
    searchSetByRef(all, sender) match {
      case Some(worker) => {
        alive = alive + worker
        responseData += (worker.workerId -> timeStamp)
      }
      case None => println("Received heartbeat from worker not supervised")
    }
  }
}
