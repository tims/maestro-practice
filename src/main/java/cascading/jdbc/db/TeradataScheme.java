package cascading.jdbc.db;

import java.sql.SQLException;
import java.util.Random;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.db.DBConfiguration;
import cascading.jdbc.db.DBOutputFormat;
import cascading.management.CascadingServices;
import cascading.scheme.SourceCall;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.util.ShutdownUtil;

public class TeradataScheme extends JDBCScheme {

  public static final String INPUT_TEMPORARY_TABLE_NAME_PROPERTY = "mapred.jdbc.input.temporary.table.name";

  public TeradataScheme(Fields columnFields, String[] columns, String[] orderBy, String conditions, long limit,
      String tempTableName) {
    super(TeradataDBInputFormat.class, DBOutputFormat.class, columnFields, columns, orderBy, conditions, limit, null,
        null, false);
    this.tempTableName = tempTableName;
  }

  private JDBCTap tap;
  private String inputQuery;
  private String createTableFromQuery;
  private String tempTableName;
  private String inputTableName;
  private String[] inputFieldNames;
  private int numPartitions = 1;
  private boolean useTemporaryTable;

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    super.sourceConfInit(process, tap, conf);
    this.tap = ((JDBCTap) tap);

    DBConfiguration dbConf = new DBConfiguration(conf);
    inputTableName = dbConf.getInputTableName();
    inputFieldNames = dbConf.getInputFieldNames();
    numPartitions = dbConf.getMaxConcurrentReadsNum() > 0 ? dbConf.getMaxConcurrentReadsNum() : 1;
    if (numPartitions > 1)
      useTemporaryTable = true;
    createTableFromQuery = getSelectQuery(dbConf, inputTableName);
    inputQuery = useTemporaryTable ? getSelectQuery(dbConf, tempTableName) : createTableFromQuery;
    dbConf.setInputQuery(inputQuery);
    conf.set(INPUT_TEMPORARY_TABLE_NAME_PROPERTY, tempTableName);
    if (useTemporaryTable)
      createTempTable();
  }

  @Override
  public boolean isSink() {
    return false;
  }

  protected String getSelectQuery(DBConfiguration dbConf, String tableName) {
    String[] fieldNames = dbConf.getInputFieldNames();
    String conditions = dbConf.getInputConditions();
    long limit = dbConf.getInputLimit();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    if (limit >= 0L)
      query.append("TOP ").append(limit).append(" ");
    query.append(selectFieldNames(fieldNames, null));
    query.append(" FROM ").append(tableName);
    if (conditions != null && conditions.length() > 0)
      query.append(" WHERE ").append(conditions);
    String orderBy = dbConf.getInputOrderBy();
    if (orderBy != null && orderBy.length() > 0) {
      query.append(" ORDER BY ").append(orderBy);
    }
    return query.toString();
  }

  private String selectFieldNames(String[] fieldNames, String prefix) {
    StringBuilder query = new StringBuilder();
    for (int i = 0; i < fieldNames.length; i++) {
      if (prefix != null)
        query.append(prefix);
      query.append(fieldNames[i]);
      if (i != fieldNames.length - 1) {
        query.append(", ");
      }
    }
    return query.toString();
  }

  private void createTempTable() {
    StringBuilder query = new StringBuilder();
    query.append("CREATE TABLE ").append(tempTableName);
    query.append(" AS ( ");
    query.append("  SELECT RANDOM(1,").append(numPartitions).append(") AS __part_id, ");
    query.append(selectFieldNames(inputFieldNames, "t1."));
    query.append(" FROM (").append(createTableFromQuery).append(") ").append(" t1) ");
    query.append("WITH DATA ");
    query.append("PRIMARY INDEX (__part_id) ");
    query.append("PARTITION BY (RANGE_N (__part_id BETWEEN 1 AND ").append(numPartitions).append("))");
    try {
      tap.executeQuery(query.toString(), 0);
    } catch (SQLException e) {
      throw new TapException("Cannot create temporary table for this Scheme", e);
    }
  }
}
