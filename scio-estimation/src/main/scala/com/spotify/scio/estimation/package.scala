/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio

import java.nio.channels.Channels

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{FileSystems, TextIO}
import org.apache.beam.sdk.options.ApplicationNameOptions
import org.apache.beam.sdk.util.CoderUtils
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

package object estimation {

  case class Estimation(count: Long, size: Long)

  /** Statistics estimator for an [[SCollection]]. */
  trait Estimator[T] {

    /** Load estimation for historical data. */
    def load(data: SCollection[T])(implicit coder: Coder[T]): Option[Estimation]

    /** Save estimation of current data. */
    def save(data: SCollection[T])(implicit coder: Coder[T]): Unit

    /** Estimate parameters for current data set and load historical ones. */
    def estimate(data: SCollection[T])(implicit coder: Coder[T]): Option[Estimation] = {
      save(data)
      load(data)
    }
  }

  trait EstimatorStrategy {
    def get[T]: Estimator[T]

    protected def sample[T: Coder](
      data: SCollection[T],
      sampleSize: Int
    ): SCollection[Estimation] = {
      val coder = CoderMaterializer.beam(data.context, implicitly[Coder[T]])
      data
        .sample(sampleSize)
        .map { xs =>
          var bytes = 0L
          var count = 0L
          xs.foreach { x =>
            bytes += CoderUtils.encodeToByteArray(coder, x).length
            count += 1
          }
          (bytes, count)
        }
        .cross(data.count)
        .map {
          case ((bytes, sampled), count) =>
            val size = (bytes.toDouble / sampled * count).toLong
            Estimation(count, size)
        }
    }
  }

  class FileBasedEstimatorStrategy(path: String, sampleSize: Int = 1000) extends EstimatorStrategy {
    private val formatter = DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss-SSS")

    override def get[T]: Estimator[T] = new Estimator[T] {
      override def load(data: SCollection[T])(implicit coder: Coder[T]): Option[Estimation] = {
        val appName = data.context.optionsAs[ApplicationNameOptions].getAppName
        val key = normalize(data.name)
        val spec = s"$path/$appName/$key/*.txt"
        try {
          val latest = FileSystems.`match`(spec).metadata().asScala.maxBy(_.resourceId().toString)
          val channel = FileSystems.open(latest.resourceId())
          val estimation = ScioUtil.getScalaJsonMapper
            .readValue(Channels.newInputStream(channel), classOf[Estimation])
          println(s"load $estimation from ${latest.resourceId().toString}")
          Some(estimation)
        } catch {
          case NonFatal(_) =>
            None
        }
      }

      override def save(data: SCollection[T])(implicit coder: Coder[T]): Unit = {
        val appName = data.context.optionsAs[ApplicationNameOptions].getAppName
        val key = normalize(data.name)
        val ts = Instant.now().toString(formatter)
        val output = s"$path/$appName/$key/$ts"
        println(s"save to $output")
        val sink = TextIO
          .write()
          .to(output)
          .withNumShards(1)
          .withShardNameTemplate("")
          .withSuffix(".txt")
        sample(data, sampleSize)
          .map(e => ScioUtil.getScalaJsonMapper.writeValueAsString(e))
          .saveAsCustomOutput("Estimation", sink)
      }
    }

    private def normalize(s: String): String =
      s.toLowerCase
        .replaceAll("[^a-z0-9]", "0")
        .replaceAll("^[^a-z]", "a")
  }

  trait JoinHeuristics {
    def join[K: Coder, V: Coder, W: Coder](
      lhs: SCollection[(K, V)],
      rhs: SCollection[(K, W)],
      lhsE: Option[Estimation],
      rhsE: Option[Estimation]
    ): SCollection[(K, (V, W))]
  }

  implicit val defaultJoinHeuristics = new JoinHeuristics {
    override def join[K: Coder, V: Coder, W: Coder](
      lhs: SCollection[(K, V)],
      rhs: SCollection[(K, W)],
      lhsE: Option[Estimation],
      rhsE: Option[Estimation]
    ): SCollection[(K, (V, W))] =
      lhs.join(rhs)
  }

  implicit class EstimationPairSCollection[K: Coder, V: Coder](val self: SCollection[(K, V)]) {
    def heuristicJoin[W: Coder](
      that: SCollection[(K, W)]
    )(implicit es: EstimatorStrategy, jh: JoinHeuristics): SCollection[(K, (V, W))] =
      jh.join(self, that, es.get.estimate(self), es.get.estimate(that))
  }

}
