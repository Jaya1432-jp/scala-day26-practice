import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object Day26 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Scala Spark Day 26 - Healthcare Streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Checkpoint is needed for stateful processing
    ssc.checkpoint("/tmp/scala-day26-checkpoint")

    val sc = ssc.sparkContext

    // Broadcast reference thresholds for vital signs
    val thresholds: Broadcast[Map[String, (Double, Double)]] =
      sc.broadcast(Map(
        "HEART_RATE" -> (60.0, 100.0),
        "SPO2"       -> (95.0, 100.0),
        "TEMPERATURE" -> (36.1, 37.5)
      ))

    // Accumulator tracks abnormal readings processed
    val abnormalReadings: LongAccumulator =
      sc.longAccumulator("Abnormal Vital Readings")

    println("==============================================")
    println("DAY 26: REAL-TIME HEALTHCARE STREAMING")
    println("==============================================")
    println("Batch interval: 5 seconds")
    println("Socket port: 9996")
    println("Input: PATIENT_ID,VITAL_TYPE,VALUE")
    println("Example: P001,HEART_RATE,120")
    println("==============================================")

    val lines = ssc.socketTextStream("localhost", 9996)

    // Parse patient vital events
    val readings = lines.flatMap { line =>
      val parts = line.split(",").map(_.trim)

      if (parts.length == 3 &&
          parts(0).nonEmpty &&
          parts(1).nonEmpty &&
          parts(2).toDoubleOption.isDefined) {

        Some((
          parts(0),
          parts(1).toUpperCase,
          parts(2).toDouble
        ))
      } else {
        None
      }
    }

    // Identify abnormal readings using broadcast thresholds
    val abnormalEvents = readings.filter {
      case (_, vitalType, value) =>
        thresholds.value.get(vitalType).exists {
          case (minimum, maximum) =>
            value < minimum || value > maximum
        }
    }

    // Count abnormal events in each micro-batch.
    // This accumulator counts processed abnormal records and may
    // increase again if Spark recomputes a task.
    abnormalEvents.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val events = rdd.collect()

        abnormalReadings.add(events.length.toLong)

        println("\n----- ABNORMAL VITAL ALERTS -----")

        events.foreach {
          case (patientId, vitalType, value) =>
            println(
              s"ALERT: Patient=$patientId, " +
              s"Vital=$vitalType, Value=$value"
            )
        }

        println(
          s"Abnormal readings in this batch: ${events.length}"
        )
        println(
          s"Accumulator total (processed abnormal records): ${abnormalReadings.value}"
        )
      }
    }

    // Stateful count of repeated abnormal readings by patient
    val abnormalByPatient = abnormalEvents
      .map { case (patientId, _, _) => (patientId, 1) }

    val repeatedAbnormalCounts = abnormalByPatient.updateStateByKey[Int](
      (newCounts: Seq[Int], previous: Option[Int]) =>
        Some(previous.getOrElse(0) + newCounts.sum)
    )

    repeatedAbnormalCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n----- CUMULATIVE ABNORMAL READINGS BY PATIENT -----")

        rdd.sortByKey().collect().foreach {
          case (patientId, count) =>
            println(s"$patientId -> $count abnormal reading(s)")

            if (count >= 3) {
              println(
                s"REPEATED-ALERT: Patient $patientId has $count abnormal readings"
              )
            }
        }
      }
    }

    // Window-based abnormal reading counts over 15 seconds
    val abnormalWindowCount = abnormalByPatient
      .map { case (_, count) => count }
      .reduceByWindow(
        (a: Int, b: Int) => a + b,
        Seconds(15),
        Seconds(5)
      )

    abnormalWindowCount.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n----- 15-SECOND ABNORMAL READING WINDOW -----")
        println(
          s"Abnormal readings in window: ${rdd.collect().head}"
        )
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
