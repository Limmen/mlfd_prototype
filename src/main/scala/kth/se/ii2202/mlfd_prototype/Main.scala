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
  val crash = opt[Double](required = true)
  val mloss = opt[Double](required = true)
  val sdev = opt[Double](required = true)
  val pattern = opt[Boolean](required = true)
  val geof = opt[Double](required = true)
  val geoc = opt[Int](required = true)
  val bwf = opt[Double](required = true)
  val bwc = opt[Int](required = true)
  val rand = opt[Boolean](required = true)
  val delta = opt[Double]()
  val pmargin = opt[Double]()
  val workers = opt[Int](required = true)
  val sdevc = opt[Int](required = true)
  val hbtimeout = opt[Double](required = true)
  val testdur = opt[Double](required = true)
  val samplew = opt[Int]()
  val defaultmean = opt[Double]()
  val defaultsdev = opt[Double]()
  val learnrate = opt[Double]()
  val regp = opt[Double]()
  val iter = opt[Int]()
  val batchsize = opt[Int]()
  val distr = opt[Int](required = true)
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

    /*
     * Read command line arguments and start testcase
     */
    val crashProb = conf.crash()
    val messageLossProb = conf.mloss()
    val sdev = conf.sdev()
    val pattern = conf.pattern()
    val geof = conf.geof()
    val geoc = conf.geoc()
    val bwf = conf.bwf()
    val bwc = conf.bwc()
    val rand = conf.rand()
    val test = conf.test()
    val workers = conf.workers()
    val sdevc = conf.sdevc()
    val hbtimeout = conf.hbtimeout()
    val testdur = conf.testdur()
    val distr = conf.distr()
    test match {
      case 1 => {
        val pmargin = conf.pmargin()
        val samplew = conf.samplew()
        val defaultmean = conf.defaultmean()
        val defaultsdev = conf.defaultsdev()
        val learnrate = conf.learnrate()
        val regp = conf.regp()
        val iter = conf.iter()
        val batchsize = conf.batchsize()
        mlfdClearCorrelationSimulation(system = system, collector = collector, crashProb = crashProb,
          messageLossProb = messageLossProb, predictionMargin = pmargin, stdevFactor = sdev,
          pattern = pattern, geof = geof, geoc = geoc, bwf = bwf, bwc = bwc, rand = rand, workersCount = workers,
          sdevc = sdevc, hbtimeout = hbtimeout, testdur = testdur, samplew = samplew, defaultmean = defaultmean,
          defaultsdev = defaultsdev, learnrate = learnrate, regp = regp, iter = iter, batchsize = batchsize, distr=distr)
      }
      case 2 => {
        val delta = conf.delta()
        epfdClearCorrelationTest(system = system, collector = collector, crashProb = crashProb, messageLossProb = messageLossProb,
          delta = delta, stdevFactor = sdev, pattern = pattern, geof = geof, geoc = geoc, bwf = bwf, bwc = bwc, rand = rand,
          workersCount = workers, sdevc = sdevc, hbtimeout = hbtimeout, testdur = testdur, distr=distr)
      }
    }
  }

  /*
   * Simulation where there is a clear correlation between geo-graphic location, bandwidth etc and RTT
   * Uses MLFD
   */
  def mlfdClearCorrelationSimulation(system: ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double,
    predictionMargin: Double, stdevFactor: Double, pattern: Boolean, geof: Double, geoc: Integer,
    bwf: Double, bwc: Integer, rand: Boolean, workersCount: Integer, sdevc: Integer, hbtimeout: Double,
    testdur: Double, samplew: Integer, defaultmean: Double, defaultsdev: Double, learnrate: Double,
    regp: Double, iter: Integer, batchsize: Integer, distr : Integer): Unit = {
    println("Starting testcase with FailureDetector MLFD")
    mlfdTest(
      workersCount = workersCount, sampleSize = samplew, defaultMean = defaultmean,
      hbTimeout = hbtimeout.seconds, system = system, collector = collector,
      stdDevCount = sdevc, geoFactor = geof, locationsCount = geoc, crashProb = crashProb,
      defaultStd = defaultsdev, bandwidthCount = bwc, bandwidthFactor = bwf,
      batchSize = batchsize, learningRate = learnrate, regParam = regp,
      numIterations = iter, testTimeout = testdur.minutes, messageLossProb = messageLossProb,
      stdevMargin = predictionMargin, pattern = pattern, stdevFactor = stdevFactor, rand = rand,
      distr=distr
    )
  }

  /*
   * Simulation where there is a clear correlation between geo-graphic location, bandwidth etc and RTT
   * Uses EPFD
   */
  def epfdClearCorrelationTest(system: ActorSystem, collector: ActorRef, crashProb: Double, messageLossProb: Double, delta: Double,
    stdevFactor: Double, pattern: Boolean, geof: Double, geoc: Integer, bwf: Double, bwc: Integer, rand: Boolean, workersCount: Integer, sdevc: Integer, hbtimeout: Double, testdur: Double, distr : Integer): Unit = {
    println("Starting testcase with FailureDetector EPFD")
    epfdTest(workersCount = workersCount, delta = delta.millis, system = system,
      stdDevCount = sdevc, geoFactor = geof, locationsCount = geoc, crashProb = crashProb,
      collector = collector, bandwidthsCount = bwc, bandwidthFactor = bwf,
      hbTimeout = hbtimeout.seconds, testTimeout = testdur.minutes,
      messageLossProb = messageLossProb, pattern = pattern, stdevFactor = stdevFactor, rand = rand,
      distr=distr)
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
   * - messageLossprob: Probability of message loss for each heartbeat reply
   * - stdevMargin: The number of standard-deviations used by MLFD as a margin for predictions
   * - pattern: True if geolocation and bandwidth should correlate with RTT delays
   * - stdevFactor: Standard deviation factor to multiply each class of standard deviation in the simulation
   * - rand: True if RTT-delay should be completely random instead of following a gaussian distribution
   * - distr: Determines distribution of RTT. 1 = Exponential, 2 = Weibull, 3 = Gaussian
   */
  def mlfdTest(workersCount: Integer, sampleSize: Integer, defaultMean: Double,
    hbTimeout: FiniteDuration, system: ActorSystem, collector: ActorRef,
    stdDevCount: Integer, geoFactor: Double, locationsCount: Integer,
    crashProb: Double, defaultStd: Double, bandwidthCount: Integer,
    bandwidthFactor: Double, batchSize: Integer, learningRate: Double, regParam: Double,
    numIterations: Integer, testTimeout: FiniteDuration, messageLossProb: Double,
    stdevMargin: Double, pattern: Boolean, stdevFactor: Double, rand: Boolean,
    distr : Integer): Unit = {

    val loc = recordGeoDistribution(locationsCount, workersCount, rand, geoFactor, collector)
    val bws = recordBwDistribution(bandwidthCount, workersCount, rand, bandwidthFactor, collector)
    val stddev = recordStdevDistribution(stdDevCount, workersCount, rand, stdevFactor, collector)
    val workers = startWorkers(n = workersCount, system = system, locations = loc,
      stdDevs = stddev, geoFactor = geoFactor, crashProb = crashProb,
      collector = collector, bandwidths = bws, bandwidthFactor = bandwidthFactor,
      messageLossProb = messageLossProb, pattern = pattern,
      altGeo = random.shuffle(loc), altBw = random.shuffle(bws), rand,
      bandwidthCount = bandwidthCount, geoCount = locationsCount, distr=distr)

    val mlfd = new MLFD(workers = workers, sampleWindowSize = 100, defaultMean = 3000.0,
      collector = collector, defaultStd = defaultStd, batchSize = batchSize, learningRate = learningRate,
      regParam = regParam, numIterations = numIterations, stdevMargin = stdevMargin)

    val superviser = system.actorOf(Superviser.props(mlfd, hbTimeout), "superviser")

    system.actorOf(Controller.props(system, testTimeout, List("mlfd_test", workersCount.toString(),
      locationsCount.toString(), sampleSize.toString(), defaultMean.toString(), hbTimeout.toString(),
      stdDevCount.toString(), geoFactor.toString(), crashProb.toString, "nil", defaultStd.toString,
      bandwidthCount.toString, bandwidthFactor.toString, batchSize.toString, learningRate.toString,
      regParam.toString, numIterations.toString(), messageLossProb.toString, stdevMargin.toString,
      stdevFactor.toString, pattern.toString, rand.toString)), "controller")
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
   * - testTimeout: duration of the test
   * - messageLossprob: Probability of message loss for each heartbeat reply
   * - stdevMargin: The number of standard-deviations used by MLFD as a margin for predictions
   * - pattern: True if geolocation and bandwidth should correlate with RTT delays
   * - stdevFactor: Standard deviation factor to multiply each class of standard deviation in the simulation
   * - rand: True if RTT-delay should be completely random instead of following a gaussian distribution
   * - distr: Determines distribution of RTT. 1 = Exponential, 2 = Weibull, 3 = Gaussian
   */
  def epfdTest(workersCount: Integer, delta: FiniteDuration, system: ActorSystem, stdDevCount: Integer,
    geoFactor: Double, locationsCount: Integer, crashProb: Double, collector: ActorRef,
    bandwidthsCount: Integer, bandwidthFactor: Double, hbTimeout: FiniteDuration,
    testTimeout: FiniteDuration, messageLossProb: Double, pattern: Boolean,
    stdevFactor: Double, rand: Boolean, distr : Integer): Unit = {

    val loc = recordGeoDistribution(locationsCount, workersCount, rand, geoFactor, collector)
    val bws = recordBwDistribution(bandwidthsCount, workersCount, rand, bandwidthFactor, collector)
    val stddev = recordStdevDistribution(stdDevCount, workersCount, rand, stdevFactor, collector)
    val workers = startWorkers(n = workersCount, system = system, locations = loc,
      stdDevs = stddev, geoFactor = geoFactor, crashProb = crashProb,
      collector = collector, bandwidths = bws, bandwidthFactor = bandwidthFactor,
      messageLossProb = messageLossProb, pattern = pattern,
      altGeo = random.shuffle(loc), altBw = random.shuffle(bws), random = rand,
      bandwidthCount = bandwidthsCount, geoCount = locationsCount, distr=distr)

    val epfd = new EPFD(workers, delta, collector, hbTimeout)

    val superviser = system.actorOf(Superviser.props(epfd, hbTimeout), "superviser")

    system.actorOf(Controller.props(system, testTimeout, List("epfd_test", workersCount.toString(),
      locationsCount.toString, "nil", "nil", hbTimeout.toString(), stdDevCount.toString(),
      geoFactor.toString(), crashProb.toString(), delta.toString(), "nil",
      bandwidthsCount.toString(), bandwidthFactor.toString, "nil", "nil", "nil", "nil",
      messageLossProb.toString, "nil", stdevFactor.toString, pattern.toString, rand.toString)), "controller")
  }

  /*
   * Utility function to start a set of worker nodes on given location and with given parameters
   */
  def startWorkers(n: Integer, system: ActorSystem, locations: List[Double], stdDevs: List[Double],
    geoFactor: Double, crashProb: Double, collector: ActorRef, bandwidths: List[Double], bandwidthFactor: Double,
    messageLossProb: Double, pattern: Boolean, altGeo: List[Double],
    altBw: List[Double], random: Boolean, bandwidthCount: Integer, geoCount: Integer, distr : Integer): List[WorkerEntry] = {
    return (1 to n).toList.map((i) => {
      val actorRef = system.actorOf(Worker.props(id = i, geoLoc = locations(i - 1), stdDev = stdDevs(i - 1), geoFactor = geoFactor, crashProb = crashProb, collector = collector, bandwidth = bandwidths(i - 1), bandwidthFactor = bandwidthFactor, messageLossProb = messageLossProb, pattern = pattern, altGeo = altGeo(i - 1), altBw = altBw(i - 1), random, bandwidthCount = bandwidthCount, geoCount = geoCount, distri=distr), i.toString)
      new WorkerEntry(actorRef, i, locations(i - 1), bandwidths(i - 1))
    })
  }

  /*
   * Utility function for associating each worker with a location given how many locations and how many workers
   */
  def locations(numLocations: Integer, numWorkers: Integer): List[Double] = {
    var locations: List[Double] = List()
    for (i <- 1 to numWorkers) {
      val loc = (i % numLocations).toDouble
      locations = loc :: locations
    }
    return random.shuffle(locations)
  }

  /*
   * Utility function for associating each worker with a bandwidth, given how many type of bandwiths and how many workers.
   */
  def bandwidths(numBandwidths: Integer, numWorkers: Integer): List[Double] = {
    var bandwidths: List[Double] = List()
    for (i <- 1 to numWorkers) {
      val bandwidth = (i % numBandwidths).toDouble
      bandwidths = bandwidth :: bandwidths
    }
    return random.shuffle(bandwidths)
  }

  /*
   * Utility function for associating each worker with a standard-deviation, given how many type of standard-deviations and how many workers.
   */
  def standardDevs(numStandardDevs: Integer, numWorkers: Integer, stdevFactor: Double): List[Double] = {
    var standardDevs: List[Double] = List()
    for (i <- 1 to numWorkers) {
      val stdDev = (((i % numStandardDevs).toDouble) + 1) * stdevFactor
      standardDevs = stdDev :: standardDevs
    }
    return random.shuffle(standardDevs)
  }

  /*
   * Write Distribution of geographic delay to file for later analysis
   */
  def recordGeoDistribution(locationsCount: Integer, workersCount: Integer, rand: Boolean,
    geoFactor: Double, collector: ActorRef): List[Double] = {
    val loc = locations(locationsCount, workersCount)
    loc.sorted.map((l: Double) => {
      var geoDelay = scala.math.pow(l, 3) * geoFactor
      if (rand) {
        val gl = random.nextInt(locationsCount)
        geoDelay = (scala.math.pow(gl.toDouble, 3) * random.nextInt(geoFactor.toInt)).toDouble
      }
      collector ! new GeoDelay(List(l.toString, (l * geoFactor).toString))
    })
    return loc
  }

  /*
   * Write Distribution of bandwidth delay to file for later analysis
   */
  def recordBwDistribution(bandwidthsCount: Integer, workersCount: Integer, rand: Boolean, bandwidthFactor: Double, collector: ActorRef): List[Double] = {
    val bws = bandwidths(bandwidthsCount, workersCount)
    bws.sorted.map((bw: Double) => {
      var bandwidthDelay = 1 * bandwidthFactor
      if (bw > 0)
        bandwidthDelay = ((1 / (scala.math.pow(bw, 3))) * bandwidthFactor)
      if (rand) {
        val bf = random.nextInt(10000)
        val b = random.nextInt(30)
        bandwidthDelay = (1 * bf).toDouble
        if (b > 0)
          bandwidthDelay = ((1 / (scala.math.pow(b.toDouble, 3)) * bf)).toDouble
      }
      collector ! new BandwidthDelay(List(bw.toString, bandwidthDelay.toString))
    })
    return bws
  }

  /*
   * Write Distribution of standard deviations to file for later analysis
   */
  def recordStdevDistribution(stdDevCount: Integer, workersCount: Integer, rand: Boolean,
    stdevFactor: Double, collector: ActorRef): List[Double] = {
    val stddev = standardDevs(stdDevCount, workersCount, stdevFactor)
    stddev.sorted.map((stddev: Double) => collector ! new Stdev(List(stddev.toString)))
    return stddev
  }
}
