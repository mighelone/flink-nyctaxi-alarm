package com.mvasce

import com.mvasce.utils.{TaxiRide, TaxiRides}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

object NYCTaxiJob {
  def main(args: Array[String]): Unit = {
    val inputPath: String = if (args.length != 1) {
      // System.err.println("Please provide the path to the Taxi ride file as a parameter!")
      "/home/michele/Codes/flink/nyctaxi/sorted_data.csv"
    } else args(1)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(1000L)

    val rides: DataStream[TaxiRide] = TaxiRides.getRides(env, inputPath)

    val notifications = rides
      .keyBy(_.licenseId)
      .process(new MonitorWorkTime)

    notifications.print()

    env.execute()
  }

  class MonitorWorkTime
      extends KeyedProcessFunction[String, TaxiRide, (String, String)] {

    private val ALLOWED_WORK_TIME = 12 * 60 * 60 * 1000L; // 12 hours
    private val REQ_BREAK_TIME = 8 * 60 * 60 * 1000L; // 8 hours
    private val CLEAN_UP_INTERVAL = 24 * 60 * 60 * 1000L; // 24 hours

    @transient lazy val formatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    lazy val shiftStart: ValueState[Long] =
      getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("shiftStart", Types.of[Long])
      )

    override def processElement(
        ride: TaxiRide,
        context: KeyedProcessFunction[String, TaxiRide, (String, String)]#Context,
        out: Collector[(String, String)]
    ): Unit = {

      var startTs = shiftStart.value()

      if (startTs == 0 || startTs < ride.pickupTime - (ALLOWED_WORK_TIME + REQ_BREAK_TIME)) {
        startTs = ride.pickupTime
        shiftStart.update(startTs)
        val endTs = startTs + ALLOWED_WORK_TIME
        val formattedTime = Instant.ofEpochMilli(endTs).toString()
        out.collect(
          (
            ride.licenseId,
            s"New shift started. Shift ends at ${formattedTime}"
          )
        )
        context
          .timerService()
          .registerEventTimeTimer(startTs + CLEAN_UP_INTERVAL)
      } else if (startTs < ride.pickupTime - ALLOWED_WORK_TIME) {
        out.collect(
          (
            ride.licenseId,
            "This ride violated the working time regulations."
          )
        )
      }
    }

    override def onTimer(
        timestamp: Long,
        // ctx: OnTimerContext,
        context: KeyedProcessFunction[String, TaxiRide, (String, String)]#OnTimerContext,
        out: Collector[(String, String)]
    ): Unit = {
      val startTs = shiftStart.value()
      if (startTs == timestamp - CLEAN_UP_INTERVAL) {
        shiftStart.clear()
      }

    }
  }

}
