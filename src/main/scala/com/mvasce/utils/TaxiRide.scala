package com.mvasce.utils

case class TaxiRide(
    medallion: String,
    licenseId: String,
    pickupTime: Long,
    dropOffTime: Long,
    pickUpLon: Float,
    pickUpLat: Float,
    dropOffLon: Float,
    dropOffLat: Float,
    total: Float
)
