package kth.se.ii2202.mlfd_prototype.ml

import com.github.tototoshi.csv.CSVWriter
import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.DecimalFormat

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, StreamingLinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }

/*
 * ML-model for predicting next timeout for a process based on RTT-samples and geo-location.
 * Uses LinearRegressionWithSGD to continously update model with new streaming data
 * - batchSize: How much data to move at once to the training-folder
 * - learningRate: learningRate of streaminglinearregressionwithsgd
 * - regParam: regParam of streaminglinearregressionwithsgd
 */
class MLFDModel(batchSize : Integer, learningRate : Double, regParam: Double, numIterations : Integer) {

  private val log = LogManager.getRootLogger
  private val numFeatures = 6
  private val trainDataPath = "data/train/" //Folder where SparkStreaming will listen for new training data
  private val tempDataFile = "data/temp/temp.txt" //write individual records to temp file before moving to training folder
  private val testDataPath = "data/test/"
  private val testResultsFilePath = "data/stats/testresults.csv"
  private val testResultsFile = new File(testResultsFilePath)
  private var batchCounter = 0
  private var (model, conf, streamContext) : (StreamingLinearRegressionWithSGD, SparkConf, StreamingContext) = init()
  private val formatter = new DecimalFormat("#.#######################################")

  /*
   * Initialize models and variables
   */
  def init(): (StreamingLinearRegressionWithSGD, SparkConf, StreamingContext) = {
    /*
   * Reduce verbosity of Spark logging
   */
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    /*
     * Create spark session, locally, use all threads, name it mlfd_prototype
     */
    val conf = new SparkConf().setMaster("local[*]").setAppName("mlfd_prototype")
    /*
     * Entry point of the spark streaming API.
     * Set batch interval to = 1 second (i.e read streaming file every 1 sec)
     */
    val ssc = new StreamingContext(conf, Seconds(1))

    //Create a stream for reading in trainingData
    val trainingData = ssc.textFileStream(trainDataPath).map(LabeledPoint.parse).cache()

    //Create a stream for reading in testData
    val testData = ssc.textFileStream(testDataPath).map(LabeledPoint.parse)

    val testResultsWriter = CSVWriter.open(testResultsFile)
    testResultsWriter.writeRow(List("meanSquaredError", "rootMeanSquaredError", "rSquared", "meanAbsoluteError", "explainedVariance", "timestamp"))
    testResultsWriter.close()

    //Initialize model (lazy) for linear-regression with SGD that will be updated with new streaming data peridocially
    model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures)).setRegParam(regParam).setStepSize(learningRate).setNumIterations(numIterations)

    //Specify that the model should be update by batches (1sec) of training data from given stream
    model.trainOn(trainingData)

    //Specify that prediction should be applied on all streaming data placed in TestData-stream, save prediction evaluations.
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).foreachRDD((rdd : RDD[(Double, Double)], time : Time) => saveTestEvaluation(rdd, time))

    //Spark is lazy, to indicate we want it to start processing we explicitly start it
    ssc.start()

    return (model, conf, ssc)
  }

  /*
   * Add a datapoint, once temp-file is of size sampleWindowsize we move it to training folder and clear up old data
   */
  def addDataPoint(dataPoint: LabeledPoint): Unit = {
    if (batchCounter < batchSize) {
      batchCounter += 1
      scala.tools.nsc.io.Path(tempDataFile).createFile().appendAll(dataPoint.toString() + "\n")
    } else {
      batchCounter = 0
      FileUtils.cleanDirectory(new File(trainDataPath))
      FileUtils.cleanDirectory(new File(testDataPath))
      val fileName = System.currentTimeMillis().toString()
      val tempFile = new File(tempDataFile).toPath
      val newTestDataFile = new File(testDataPath + "/" + fileName).toPath
      Files.copy(tempFile, newTestDataFile, StandardCopyOption.REPLACE_EXISTING)
      val newTrainingFile = new File(trainDataPath + "/" + fileName).toPath
      Files.move(tempFile, newTrainingFile, StandardCopyOption.ATOMIC_MOVE)
    }
  }

  /*
   * Save evaluation metrics
   */
  def saveTestEvaluation(rdd : RDD[(Double, Double)], time: Time) : Unit = {
    if(rdd.count() > 0){
      //Instantiate metrics object
      val metrics = new RegressionMetrics(rdd)
      val testResultsWriter = CSVWriter.open(testResultsFile, append=true)
      testResultsWriter.writeRow(List(metrics.meanSquaredError.toString, metrics.rootMeanSquaredError.toString, metrics.r2.toString, metrics.meanAbsoluteError.toString, metrics.explainedVariance.toString, formatter.format(time.milliseconds.toDouble)))
      testResultsWriter.close()
      log.debug(s"MSE = ${metrics.meanSquaredError}")
      //Squared error
      log.debug(s"RMSE = ${metrics.rootMeanSquaredError}")
      //R-squared
      log.debug(s"R-squared = ${metrics.r2}")
      //Mean absolute error
      log.debug(s"MAE = ${metrics.meanAbsoluteError}")
      //Explained variance
      log.debug(s"Explained variance = ${metrics.explainedVariance}")
    }
  }

  /*
   * Predict next timeout of process based on mean and geo-location by using the current trained model
   */
  def predict(mean: Double, sdev: Double, geoLoc: Double, min: Double, max: Double, bandwidth: Double): Double = {
    val linearModel: LinearRegressionModel = model.latestModel()
    log.debug("Model Weights: " + linearModel.weights)
    return linearModel.predict(Vectors.dense(mean, sdev, geoLoc, min, max, bandwidth))
  }

}
