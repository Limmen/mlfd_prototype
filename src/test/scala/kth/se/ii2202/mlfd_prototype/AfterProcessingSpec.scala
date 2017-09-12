import org.scalatest._
import java.text.DecimalFormat
import kth.se.ii2202.mlfd_prototype.actors.Controller

class AfterProcessingSpec extends FlatSpec {
  private val formatter = new DecimalFormat("########################.#######################################")

  val suspicion_data = List(
    List("52", "1505073563445"),
    List("52", "2505073563445"),
    List("35", "1505073561456"),
    List("96", "1505073905476"),
    List("17", "3050739054760"),
    List("17", "2050739054760"),
    List("63", "14050736077470")
  )
  val crash_data = List(
    List("52", "1505073567536"),
    List("35", "1505073579610"),
    List("17", "1505073587648"),
    List("63", "1505073607747")
  )

  "GetDetectionsOfCrashes" should "return the latest detection matching the crashes" in {
    val detections = Controller.getDetectionsOfCrashes(suspicion_data, crash_data)
    assert(detections.length == 4)
    assert(detections.contains(List("17", "3050739054760")))
    assert(detections.contains(List("35", "1505073561456")))
    assert(detections.contains(List("52", "2505073563445")))
    assert(detections.contains(List("63", "14050736077470")))
  }

  "calculateDetectionTimes" should "return the detection times" in {
    val detections = Controller.getDetectionsOfCrashes(suspicion_data, crash_data)
    val detectionTimes = Controller.calculateDetectionTimes(detections, crash_data)
    assert(detectionTimes.length == 4)
    var decTime: Double = "3050739054760".toDouble - "1505073587648".toDouble
    assert(detectionTimes.contains(List("17", formatter.format(decTime), "1505073587648")))
    assert(detectionTimes.contains(List("35", formatter.format(0.0), "1505073579610")))
    decTime = "2505073563445".toDouble - "1505073567536".toDouble
    assert(detectionTimes.contains(List("52", formatter.format(decTime), "1505073567536")))
    decTime = "14050736077470".toDouble - "1505073607747".toDouble
    assert(detectionTimes.contains(List("63", formatter.format(decTime), "1505073607747")))
  }

  "calculateFalseSuspicions" should "return the false suspicions" in {
    val detections = Controller.getDetectionsOfCrashes(suspicion_data, crash_data)
    val falseSuspicions = Controller.calculateFalseSuspicions(suspicion_data, detections, crash_data)
    assert(falseSuspicions.length == 3)
    assert(falseSuspicions.contains(List("52", "1505073563445")))
    assert(falseSuspicions.contains(List("96", "1505073905476")))
    assert(falseSuspicions.contains(List("17", "2050739054760")))
    assert(!falseSuspicions.contains(List("17", "3050739054760")))
  }

  "crashCount" should "return the number of crashed processes per timestamp" in {
    val crashCount = Controller.crashCount(crash_data)
    assert(crashCount.length == 4)
    assert(crashCount.contains(List("1505073567536", "1")))
    assert(crashCount.contains(List("1505073579610", "2")))
    assert(crashCount.contains(List("1505073587648", "3")))
    assert(crashCount.contains(List("1505073607747", "4")))
  }
}
