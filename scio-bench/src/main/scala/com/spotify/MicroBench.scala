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

object MicroBench {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val name = args("name")
    val cls = Class.forName(s"com.spotify.$name")

    cls.newInstance().asInstanceOf[MicroBench].run(sc)
    sc.close()
  }
}

trait MicroBench {
  def run(sc: ScioContext): Unit

  implicit class RichScioContext(val self: ScioContext) {
    def seq: SCollection[Long] = {
      val t = GenerateSequence
        .from(0)
        .to(1000000 * 20)
      self.customInput("sequence", t).asInstanceOf[SCollection[Long]]
    }
  }
}

class MapMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.seq.map(identity)
}

class FlatMapMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.seq.flatMap(Seq(_))
}

class FilterMicroBench extends MicroBench {
  override def run(sc: ScioContext): Unit = sc.seq.filter(_ % 2 == 0)
}
