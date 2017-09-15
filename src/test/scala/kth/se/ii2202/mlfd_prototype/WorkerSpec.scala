package kth.se.ii2202.mlfd_prototype

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit}
import kth.se.ii2202.mlfd_prototype.actors._
import kth.se.ii2202.mlfd_prototype.actors.Worker._
import kth.se.ii2202.mlfd_prototype.actors.Superviser.HeartBeat
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike, MustMatchers}


class WorkerSpec() extends TestKit(ActorSystem("WorkerSpec"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterAll {

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

    "A Worker with 0% crash" must {
      "respond to a HeartBeat with a reply" in {
      val worker = TestActorRef(new Worker(id=1, geoLoc=1, stdDev=1, geoFactor=0, crashProb=0, collector = self, bandwidth=1, bandwidthFactor=0, messageLossProb=0, pattern=true, 0, 1, false, 1, 1, 3))
      worker ! HeartBeat
      expectMsg(HeartBeatReply(1,1,1))
    }
  }
}
