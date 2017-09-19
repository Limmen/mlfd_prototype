package kth.se.ii2202.mlfd_prototype.fds

import akka.actor.ActorRef
import kth.se.ii2202.mlfd_prototype.actors.Superviser.WorkerEntry
import kth.se.ii2202.mlfd_prototype.actors.Worker.HeartBeatReply

/*
 * FailureDetector trait, all FDs should extend this for easy testing
 */
trait FD {

  def timeout(): Set[WorkerEntry]
  def receivedReply(reply: HeartBeatReply, sender: ActorRef): Unit

  def searchSetByRef(workerSet: Set[WorkerEntry], ref: ActorRef): Option[WorkerEntry] = {
    workerSet.map((worker: WorkerEntry) => if (worker.actorRef == ref) return Some(worker))
    return None
  }
}
