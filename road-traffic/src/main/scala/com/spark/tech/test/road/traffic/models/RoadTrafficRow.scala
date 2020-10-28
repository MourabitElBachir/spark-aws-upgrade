package com.spark.tech.test.road.traffic.models

import java.sql.{Date, Timestamp}

case class RoadTrafficRow(dateReferentiel: Date,
                          route: String,
                          longueur: Double,
                          prD: Int,
                          depPrD: Int,
                          concessionPrD: String,
                          absD: Double,
                          cumulD: Double,
                          xD: Double,
                          yD: Double,
                          zD: Double,
                          prF: Int,
                          depPrF: Int,
                          concessionPrF: String,
                          absF: Double,
                          cumulF: Double,
                          xF: Double,
                          yF: Double,
                          zF: Double,
                          anneeMesureTrafic: Int,
                          typeComptageTrafic: String,
                          TMJA: Int,
                          RatioPL: Double)
