package practice

import org.junit.runner.RunWith
import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import au.com.cba.omnia.thermometer.core.ThermometerRecordReader
import scalaz.effect.IO
import au.com.cba.omnia.thermometer.context.Context

@RunWith(classOf[JUnitRunner])
object PracticeSpec extends ThermometerSpec {
  def is = s2"""
Export job spec
===============

  test things $things
  
"""

  val pipeline = withArgs(Map("input" -> "input", "output" -> "output"))(new PracticeJob(_))

  val reader = ThermometerRecordReader[String]((conf, path) => IO {
    new Context(conf).lines(path)
  })

  def things = withEnvironment(path(getClass.getResource("env").getPath()))(
    pipeline.withFacts(
      "output" </> "*" ==> records(reader, reader, "expected" </> "*")))
}