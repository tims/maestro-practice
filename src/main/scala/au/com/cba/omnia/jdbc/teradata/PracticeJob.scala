package au.com.cba.omnia.jdbc.teradata

import com.twitter.scalding.{ Args, Job, Tsv }

import au.com.cba.omnia.thermometer.tools.{ HadoopSupport, ScaldingSupport }

import cascading.tuple.Fields

class PracticeJob(args: Args) extends Job(args) {
  val connectionUrl = args("connectionUrl")
  val user = args("user")
  val password = args("password")
  val output = args("output")
  val splits = Integer.valueOf(args.getOrElse("splits", "3"))

  case class PracticeSource() extends TeradataSource(connectionUrl, user, password) {
    override def tempTableName = "practice1__temporary"
    override def tableName = "practice1"
    override def columnFields = new Fields("col1", "col2")
    override def columns = Array("col1", "col2")
    override def limit = 100
    override def maxConcurrentReads = splits
  }
  PracticeSource().write(Tsv(output))
}

/*
object Hi extends HadoopSupport with ScaldingSupport {
  def main(args: Array[String]): Unit = {
    println("Hi!")
    val args = scaldingArgs +
      ("user" -> List("tim")) +
      ("password" -> List("password")) +
      ("connectionUrl" -> List("jdbc:teradata://ec2-54-208-132-244.compute-1.amazonaws.com/TMODE=ANSI,CHARSET=UTF8,DATABASE=tim")) +
      ("output" -> List("/Users/tim/output.txt"))

    val job = new PracticeJob(args)
    job.run
  }
}
*/