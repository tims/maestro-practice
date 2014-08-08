package au.com.cba.omnia.jdbc.teradata

import com.twitter.scalding.{ AccessMode, Mode, Source }
import cascading.jdbc.{ JDBCTap, TableDesc }
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.jdbc.db.DBOutputFormat
import cascading.jdbc.db.TeradataScheme;

class TeradataDBOutputFormat extends DBOutputFormat {}

class TeradataSource(connectionUrl: String, username: String, password: String) extends Source {
  def tempTableName: String = null
  def tableName: String = null
  def columnFields: Fields = null
  def columns: Array[String] = null
  def orderBy: Array[String] = null
  def conditions: String = null
  def limit: Long = -1
  def maxConcurrentReads: Int = 1

  def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tableDesc: TableDesc = new TableDesc(tableName)
    val scheme = new TeradataScheme(columnFields, columns, orderBy, conditions, limit, tempTableName)
    val driverClassName: String = "com.teradata.jdbc.TeraDriver"

    val tap = new JDBCTap(connectionUrl, username, password, driverClassName, tableDesc, scheme)
    tap.setConcurrentReads(maxConcurrentReads);
    tap.asInstanceOf[Tap[_, _, _]]
  }

}

