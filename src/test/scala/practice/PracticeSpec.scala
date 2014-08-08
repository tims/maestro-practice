package practice

import au.com.cba.omnia.jdbc.teradata.PracticeJob;
import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._

import com.twitter.scalding.Tsv

import au.com.cba.omnia.thermometer.fact.PathFactoid

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
object PracticeSpec extends ThermometerSpec {
  def is = s2"""
Export job spec
===============

  test export $export
  
"""
  
  val pipeline = withArgs(Map())(new PracticeJob(_))
  def export = pipeline.withFacts(
    path("/Users/tim/output.txt") ==> PathFactoid((context, path) => {
      context.lines(path).map(l => println(l))
      true
    }))
}