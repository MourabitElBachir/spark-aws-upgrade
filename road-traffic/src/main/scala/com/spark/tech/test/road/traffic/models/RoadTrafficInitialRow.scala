package com.spark.tech.test.road.traffic.models

import java.sql.{Date, Timestamp}

case class RoadTrafficInitialRow (dateReferentiel: Date,
                             route: String,
                             longueur: String,
                             prD: Int,
                             depPrD: Int,
                             concessionPrD: String,
                             absD: String,
                             cumulD: String,
                             xD: String,
                             yD: String,
                             zD: String,
                             prF: Int,
                             depPrF: Int,
                             concessionPrF: String,
                             absF: String,
                             cumulF: String,
                             xF: String,
                             yF: String,
                             zF: String,
                             anneeMesureTrafic: Int,
                             typeComptageTrafic: String,
                             TMJA: Int,
                             RatioPL: String)
