package kth.se.ii2202.mlfd_prototype

import akka.actor.ActorRef
import kth.se.ii2202.mlfd_prototype.fds.MLFD
import scala.concurrent.duration._

import akka.actor.ActorSystem
import kth.se.ii2202.mlfd_prototype.actors._
import kth.se.ii2202.mlfd_prototype.actors.Superviser._
import kth.se.ii2202.mlfd_prototype.fds._
import kth.se.ii2202.mlfd_prototype.actors.DataCollector._

/*
 * Project entrypoint
 */
object Main {

  /*
   * Start Akka system and start simulations/tests
   */
  def main(args: Array[String]) = {

    val system: ActorSystem = ActorSystem("mlfd_prototype")

    val collector = system.actorOf(DataCollector.props, "dataCollector")

    mlfdTest(workersCount = 100, sampleSize = 100, defaultMean = 3000.0, hbTimeout=4.seconds, system=system, collector=collector, randomMillis=200, geoFactor=100.0, locationsCount=100, crashProb=0.001, defaultStd=1000.0)

    //epfdTest(workersCount=100, delta=500.millis, system=system, randomMillis=200, geoFactor=100.0, locationsCount=100, crashProb=0.001, collector=collector)
  }


  /*
   * Perform test/simulation with the MLFD failure detector and a bunch of tunable parameters:
   * - workersCount: number of nodes to failure detect on
   * - sampleSize: number of latest samples that the FD uses to compute mean and standard deviation
   * - defaultMean: the mean used before any sample has been recorded
   * - hbTimeout: static periodic heartbeat that is sent by the failure detector
   * - system: the akka system
   * - collector: process who coordinates collecting data from the simulation and write to csv
   * - randomMillis: basically how much standard deviation in response time from each worker
   * - geoFactor: how much does the geographic distance affect the RTT
   * - locationsCount: how many simulated geographic locations to deistribute the nodes on
   * - crashProb: probability for each heart-beat received that a worker will crash
   * - defaultStd: Standard-deviation to use for prediction when not enough samples have been collected
   */
  def mlfdTest(workersCount: Integer, sampleSize: Integer, defaultMean: Double, hbTimeout: FiniteDuration, system : ActorSystem, collector: ActorRef, randomMillis:Integer, geoFactor: Double, locationsCount : Integer, crashProb : Double, defaultStd : Double) : Unit = {
    val workers = startWorkers(workersCount, system, locations(locationsCount, workersCount), randomMillis, geoFactor, crashProb, collector)
    val mlfd = new MLFD(workers, 100, 3000.0, hbTimeout, collector, defaultStd)
    val superviser = system.actorOf(Superviser.props(mlfd, hbTimeout), "superviser")
    collector ! new SimulationInfo(List("mlfd_test", workersCount.toString(), locationsCount.toString(), sampleSize.toString(), defaultMean.toString(), hbTimeout.toString(), randomMillis.toString(), geoFactor.toString(), crashProb.toString))
  }


  /*
   * Perform test/simulation with the EPFD failure detector and a bunch of tunable parameters:
   * - workersCount: number of nodes to failure detect on
   * - delta: Milliseconds to increase timeout for each premature failure-detection
   * - system: the akka system
   * - randomMillis: basically how much standard deviation in response time from each worker
   * - geoFactor: how much does the geographic distance affect the RTT
   * - locationsCount: how many simulated geographic locations to deistribute the nodes on
   * - crashProb: probability for each heart-beat received that a worker will crash
   * - collector: process who coordinates collecting data from the simulation and write to csv
   */
  def epfdTest(workersCount: Integer, delta: FiniteDuration, system : ActorSystem, randomMillis:Integer, geoFactor: Double, locationsCount : Integer, crashProb : Double, collector : ActorRef) : Unit = {
    val workers = startWorkers(workersCount, system, locations(locationsCount, workersCount), randomMillis, geoFactor, crashProb, collector)
    val epfd = new EPFD(workers, delta, collector)
    val superviser = system.actorOf(Superviser.props(epfd, delta), "superviser")
    collector ! new SimulationInfo(List("epfd_test", workersCount.toString(), locationsCount.toString, "nil", "nil", "nil", randomMillis.toString(), geoFactor.toString(), crashProb.toString(), delta.toString()))
  }


  /*
   * Utility function to start a set of worker nodes on given location and with given parameters
   */
  def startWorkers(n: Integer, system : ActorSystem, locations : List[Integer], randomMillis : Integer, geoFactor : Double, crashProb : Double, collector: ActorRef) : List[WorkerEntry] = {
        return (1 to n).toList.map((i) => {
          val actorRef = system.actorOf(Worker.props(i, locations(i-1), randomMillis, geoFactor, crashProb, collector), i.toString)
          new WorkerEntry(actorRef, i, i)
    })
  }

  /*
   * Utility function for associating each worker with a location given how many locations and how many workers
   */
  def locations(numLocations : Integer, numWorkers : Integer) : List[Integer] = {
    var locations : List[Integer] = List()
    for (i <- 1 to numWorkers) {
      val loc = i % numLocations
      locations = loc :: locations
    }
    return locations.reverse
  }
}
