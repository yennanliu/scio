import com.spotify.scio._
import org.apache.beam.sdk.io.GenerateSequence

object FilterBenchmark {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val million = 1000000
    sc.customInput(GenerateSequence.from(0).to())
    sc.close()
  }
}
