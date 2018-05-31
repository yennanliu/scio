/*
 * Copyright 2018 Spotify AB.
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

package com.spotify

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.Duration

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MicroBench {

  private var duration: Int = 0

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    duration = args.int("duration")
    val name = args("name")
    val cls = Class.forName(s"com.spotify.$name")

    cls.newInstance().asInstanceOf[MicroBench].run(sc)
    Future(sc.close())
    Thread.sleep(duration * 1000)
    sys.runtime.halt(0)
  }
}

trait MicroBench {
  def run(sc: ScioContext): Unit

  implicit class RichScioContext(val self: ScioContext) {
    def inf: SCollection[Long] = {
      val t = GenerateSequence
        .from(0)
        .withMaxReadTime(Duration.standardSeconds(MicroBench.duration))
      self.customInput("sequence", t).asInstanceOf[SCollection[Long]]
    }
  }

  implicit class RichSCollection[T](val self: SCollection[T]) {
    def blackhole: Unit = {
      implicit val ct = self.ct
      self.applyTransform(ParDo.of(new DoFn[T, T] {
        @ProcessElement
        private[spotify] def processElement(ctx: DoFn[T, T]#ProcessContext): Unit = {
          ctx.element()
        }
      }))
    }
  }
}

class MapMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.inf.map(identity).blackhole
}

class FlatMapMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.inf.flatMap(Seq(_)).blackhole
}

class FilterMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.inf.filter(_ % 2 == 0).blackhole
}
