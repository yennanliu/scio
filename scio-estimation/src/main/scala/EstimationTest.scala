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
import com.spotify.scio._
import com.spotify.scio.estimation._
object EstimationTest {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val lhs = sc.parallelize(1 to 100).map(i => (s"key$i", s"val$i"))
    val rhs = sc.parallelize(1 to 10).map(i => (s"key$i", i))

    implicit val e = new FileBasedEstimatorStrategy("/tmp/es")
    lhs.heuristicJoin(rhs)

    sc.close()
    ()
  }
}
