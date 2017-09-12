package kth.se.ii2202.mlfd_prototype

import akka.actor.ActorRef
import kth.se.ii2202.mlfd_prototype.fds.MLFD
import org.rogach.scallop.ScallopConf
import scala.concurrent.duration._

import akka.actor.ActorSystem
import kth.se.ii2202.mlfd_prototype.actors._
import kth.se.ii2202.mlfd_prototype.actors.Superviser._
import kth.se.ii2202.mlfd_prototype.actors.DataCollector._
import kth.se.ii2202.mlfd_prototype.fds._

/*
 * Parser of command-line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val test = opt[Int](required = true)
  val crash = opt[Double](required=true)
  val mloss = opt[Double](required=true)
  verify()
}

/*
 * Project entrypoint
 */
object Main {

  val random = new scala.util.Random(1000)
  /*
   * Start Akka system and start simulations/tests
   */
  def main(args: Array[String]) = {

    val conf = new Conf(args)

    val system: ActorSystem = ActorSystem("mlfd_prototype")

    val collector = system.actorOf(DataCollector.props, "dataCollector")

    val crashProb = conf.crash()
    val messageLossProb = conf.mloss()
    val test = conf.test()
    test match {
      case 1 => mlfdClearCorrelationSimulation(system: ActorSystem, collector: ActorRef, crashProb, messageLossProb)
      case 2 => epfdClearCorrelationTest(system: ActorSystem, collector: ActorRef, crashProb, messageLossProb)
      case 3 => mlfdRandomCorrelationTest(system: ActorSystem, collector: ActorRef, crashProb, messageLossProb)
      case 4 => epfdRandomCorrelationTest(system: ActorSystem, collector: ActorRef, crashProb, messageLossProb)
    }

  }

  /*
   * Simulation where there is a clear correlation between geo-graphic location, bandwidth etc and RTT
   * Uses MLFD
   */
  def mlfdClearCorrelationSimulation(system : ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double) : Unit = {
    println("Starting testcase: ClearCorrelationSimulation with FailureDetector: MLFD")
    mlfdTest(
      workersCount = 100, sampleSize = 200, defaultMean = 3000.0,
      hbTimeout=2.seconds, system=system, collector=collector,
      stdDevCount=30, geoFactor=100.0, locationsCount=30, crashProb=crashProb,
      defaultStd=1000.0, bandwidthCount=10, bandwidthFactor=10000.0,
      batchSize=100, learningRate=0.00000001, regParam=0.3,
      numIterations=10, testTimeout = 2.minutes, messageLossProb=messageLossProb,
      stdevMargin=2, pattern=true)
  }

  /*
   * Simulation where there is a clear correlation between geo-graphic location, bandwidth etc and RTT
   * Uses EPFD
   */
  def epfdClearCorrelationTest(system : ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double) : Unit = {
    println("Starting testcase: ClearCorrelationSimulation with FailureDetector: EPFD")
    epfdTest(workersCount=100, delta=500.millis, system=system,
      stdDevCount=100, geoFactor=100.0, locationsCount=100, crashProb=crashProb,
      collector=collector, bandwidthsCount=100, bandwidthFactor=1000.0,
      hbTimeout=4.seconds, testTimeout= 10.minutes, messageLossProb=messageLossProb, pattern=true)
  }

  /*
   * Simulation where there are no correlation at all between geo-graphic location, bandwidth etc and RTT
   * Uses MLFD
   */
  def mlfdRandomCorrelationTest(system : ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double) : Unit = {
    println("Starting testcase: RandomCorrelationSimulation with FailureDetector: MLFD")
    mlfdTest(
      workersCount = 100, sampleSize = 200, defaultMean = 3000.0,
      hbTimeout=2.seconds, system=system, collector=collector,
      stdDevCount=30, geoFactor=100.0, locationsCount=30, crashProb=crashProb,
      defaultStd=1000.0, bandwidthCount=10, bandwidthFactor=10000.0,
      batchSize=100, learningRate=0.00000001, regParam=0.3,
      numIterations=10, testTimeout = 50.minutes, messageLossProb=messageLossProb,
      stdevMargin=2, pattern=false)

  }

  /*
   * Simulation where there are no correlation at all between geo-graphic location, bandwidth etc and RTT
   * Uses EPFD
   */
  def epfdRandomCorrelationTest(system : ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double) : Unit = {
    println("Starting testcase: RandomCorrelationSimulation with FailureDetector: EPFD")
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
   * - bandwidthCount: how many simulated type of bandwidths
   * - bandwidthFactor: how much does the bandwidth affect the RTT
   * - testTimeout: duration of the test
   */
  def mlfdTest(workersCount: Integer, sampleSize: Integer, defaultMean: Double,
    hbTimeout: FiniteDuration, system : ActorSystem, collector: ActorRef,
    stdDevCount:Integer, geoFactor: Double, locationsCount : Integer,
    crashProb : Double, defaultStd : Double, bandwidthCount: Integer,
    bandwidthFactor : Double, batchSize: Integer, learningRate : Double, regParam : Double,
    numIterations : Integer, testTimeout: FiniteDuration, messageLossProb : Double,
    stdevMargin: Double, pattern: Boolean) : Unit = {

    val loc = locations(locationsCount, workersCount)
    loc.sorted.map((l:Double) => collector ! new GeoDelay(List(l.toString, (l * geoFactor).toString)))
    val bws = bandwidths(bandwidthCount, workersCount)
    bws.sorted.map((bw:Double) => {
      var bandwidthDelay = 1 * bandwidthFactor
      if(bw > 0)
        bandwidthDelay = ((1/bw) * bandwidthFactor)
      collector ! new BandwidthDelay(List(bw.toString, bandwidthDelay.toString))
    })
    val stddev=standardDevs(stdDevCount, workersCount)
    stddev.sorted.map((stddev:Double) => collector ! new Stdev(List(stddev.toString)))

    val workers = startWorkers(n=workersCount, system=system, locations=loc,
      stdDevs=stddev, geoFactor=geoFactor, crashProb=crashProb,
      collector=collector, bandwidths=bws, bandwidthFactor=bandwidthFactor,
      messageLossProb=messageLossProb, pattern=pattern)

    val mlfd = new MLFD(workers = workers, sampleWindowSize = 100,defaultMean=3000.0, collector=collector, defaultStd=defaultStd, batchSize=batchSize, learningRate=learningRate, regParam=regParam, numIterations=numIterations, stdevMargin=stdevMargin)

    val superviser = system.actorOf(Superviser.props(mlfd, hbTimeout), "superviser")

    system.actorOf(Controller.props(system, testTimeout, List("mlfd_test", workersCount.toString(), locationsCount.toString(), sampleSize.toString(),
      defaultMean.toString(), hbTimeout.toString(), stdDevCount.toString(), geoFactor.toString(),
      crashProb.toString,"nil", defaultStd.toString, bandwidthCount.toString, bandwidthFactor.toString,
      batchSize.toString, learningRate.toString, regParam.toString, numIterations.toString(), messageLossProb.toString, stdevMargin.toString)), "controller")
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
   * - bandwidthCount: how many simulated type of bandwidths
   * - bandwidthFactor: how much does the bandwidth affect the RTT
   * - hbTimeout: static periodic heartbeat that is sent by the failure detector
   * - testTimeout: duration of the test
   */
  def epfdTest(workersCount: Integer, delta: FiniteDuration, system : ActorSystem, stdDevCount:Integer,
    geoFactor: Double, locationsCount : Integer, crashProb : Double, collector : ActorRef,
    bandwidthsCount: Integer, bandwidthFactor: Double, hbTimeout: FiniteDuration,
    testTimeout: FiniteDuration, messageLossProb : Double, pattern: Boolean) : Unit = {

    val loc = locations(locationsCount, workersCount)
    loc.map((l:Double) => collector ! new GeoDelay(List(l.toString, (l * geoFactor).toString)))
    val bws = bandwidths(bandwidthsCount, workersCount)
    bws.map((bw:Double) => {
      var bandwidthDelay = 1 * bandwidthFactor
      if(bw > 0)
        bandwidthDelay = ((1/bw) * bandwidthFactor)
      collector ! new BandwidthDelay(List(bw.toString, bandwidthDelay.toString))
    })
    val stddev=standardDevs(stdDevCount, workersCount)
    stddev.map((stddev:Double) => collector ! new Stdev(List(stddev.toString)))
    val workers = startWorkers(n=workersCount, system=system, locations=loc,
      stdDevs=stddev, geoFactor=geoFactor, crashProb=crashProb,
      collector=collector, bandwidths=bws, bandwidthFactor=bandwidthFactor,
      messageLossProb=messageLossProb, pattern=pattern)

    val epfd = new EPFD(workers, delta, collector, hbTimeout)

    val superviser = system.actorOf(Superviser.props(epfd, hbTimeout), "superviser")

    system.actorOf(Controller.props(system, testTimeout, List("epfd_test", workersCount.toString(), locationsCount.toString, "nil", "nil",
      hbTimeout.toString(), stdDevCount.toString(), geoFactor.toString(), crashProb.toString(), delta.toString(),
      "nil", bandwidthsCount.toString(), bandwidthFactor.toString, "nil", "nil", "nil", "nil", messageLossProb.toString, "nil")), "controller")
  }


  /*
   * Utility function to start a set of worker nodes on given location and with given parameters
   */
  def startWorkers(n: Integer, system : ActorSystem, locations : List[Double], stdDevs : List[Double], geoFactor : Double, crashProb : Double, collector: ActorRef, bandwidths: List[Double], bandwidthFactor: Double, messageLossProb:Double, pattern: Boolean) : List[WorkerEntry] = {
        return (1 to n).toList.map((i) => {
          val actorRef = system.actorOf(Worker.props(id= i, geoLoc=locations(i-1), stdDev = stdDevs(i-1), geoFactor=geoFactor, crashProb=crashProb, collector=collector, bandwidth=bandwidths(i-1), bandwidthFactor=bandwidthFactor,messageLossProb=messageLossProb, pattern=pattern), i.toString)
          new WorkerEntry(actorRef, i, locations(i-1), bandwidths(i-1))
    })
  }

  /*
   * Utility function for associating each worker with a location given how many locations and how many workers
   */
  def locations(numLocations : Integer, numWorkers : Integer) : List[Double] = {
    var locations : List[Double] = List()
    for (i <- 1 to numWorkers) {
      val loc = (i % numLocations).toDouble
      locations = loc :: locations
    }
    return random.shuffle(locations)
  }

  /*
   * Utility function for associating each worker with a bandwidth, given how many type of bandwiths and how many workers.
   */
  def bandwidths(numBandwidths : Integer, numWorkers : Integer) : List[Double] = {
    var bandwidths : List[Double] = List()
    for (i <- 1 to numWorkers) {
      val bandwidth = (i % numBandwidths).toDouble
      bandwidths = bandwidth :: bandwidths
    }
    return random.shuffle(bandwidths)
  }

  /*
   * Utility function for associating each worker with a standard-deviation, given how many type of standard-deviations and how many workers.
   */
  def standardDevs(numStandardDevs : Integer, numWorkers : Integer) : List[Double] = {
    var standardDevs : List[Double] = List()
    for (i <- 1 to numWorkers) {
      val stdDev = (((i % numStandardDevs).toDouble)+1)*10
      standardDevs = stdDev :: standardDevs
    }
    return random.shuffle(standardDevs)
  }
}
