package com.mvasce

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.mvasce.utils.TaxiRide
import com.mvasce.utils.TaxiRides

object NYCTaxiJob {
    def main(args: Array[String]): Unit = {
        val inputPath: String = if (args.length != 1) {
            // System.err.println("Please provide the path to the Taxi ride file as a parameter!")
            "/home/michele/Codes/flink/nyctaxi/sorted_data.csv"
        } else args(1)

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.getConfig.setAutoWatermarkInterval(1000L)

        val rides: DataStream[TaxiRide] = TaxiRides.getRides(env, inputPath)

        rides.print()

        env.execute()

    }
}