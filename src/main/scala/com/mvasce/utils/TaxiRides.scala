package com.mvasce.utils


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.apache.flink.api.common.typeinfo.Types
// import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor

object TaxiRides {
    
    def getRides(env: StreamExecutionEnvironment, csvFile: String): DataStream[TaxiRide]= {

        // https://stackoverflow.com/questions/52082340/read-csv-with-more-than-22-colums-in-apache-flink
        val inputFieldTypes: Array[TypeInformation[_]] = Array(
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.FLOAT,
            Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT,
            Types.STRING, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT
        )

        val inputFormat = new RowCsvInputFormat(
            null,
            inputFieldTypes,
            "\n",
            ","
        )
        val parsedRow = env
            .readFile(inputFormat, filePath = csvFile).setParallelism(1)

        parsedRow.map(new RideMapper) //.assignTimestampsAndWatermarks((ride: TaxiRide)=> ride.dropOffTime)            
    }

    class RideMapper extends RichMapFunction[Row, TaxiRide] {
        @transient lazy val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

        // override def open(parameters: Configuration): Unit = {
        //     super.open(parameters)
        // }

        override def map(row: Row): TaxiRide = {
            val pickUpTime = formatter.parseDateTime(row.getField(2).toString()).getMillis()
            val dropOffTime = formatter.parseDateTime(row.getField(3).toString()).getMillis()

            TaxiRide(
                row.getField(0).toString(),
                row.getField(1).toString(),
                pickUpTime,
                dropOffTime,
                row.getField(4).toString().toFloat,
                row.getField(5).toString().toFloat,
                row.getField(6).toString().toFloat,
                row.getField(7).toString().toFloat,
                row.getField(8).toString().toFloat,
            )
        }
    }

    // class RideWatermark extends AscendingTimestampExtractor[TaxiRide] {
    //     override def extractAscendingTimestamp(ride: TaxiRide) = ride.dropOffTime
    // }
}
