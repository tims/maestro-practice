package practice

import com.twitter.scalding.{ Args, Job, Tsv }

class PracticeJob(args: Args) extends Job(args) {
  val input = args("input")
  val output = args("output")

  Tsv(input).write(Tsv(output))
}
