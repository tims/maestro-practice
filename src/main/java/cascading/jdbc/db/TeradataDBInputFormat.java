package cascading.jdbc.db;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.google.common.collect.Lists;

public class TeradataDBInputFormat extends DBInputFormat<DBWritable> {
  private void openConnection() {
    try {
      connection = dbConf.getConnection();
    } catch (IOException exception) {
      throw new RuntimeException("Unable to create connection", exception.getCause());
    }
    setTransactionIsolationLevel(connection);
    setAutoCommit(connection);
  }

  private void closeConnection() throws IOException {
    if (connection != null) {
      try {
        connection.commit();
        connection.close();
        connection = null;
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
    String tempTableName = job.get(TeradataScheme.INPUT_TEMPORARY_TABLE_NAME_PROPERTY);
    int numSplits = dbConf.getMaxConcurrentReadsNum();
    if (tempTableName == null || numSplits <= 1) {
      InputSplit split = new TeradataInputSplit(1, 10);
      return Lists.newArrayList(split).toArray(new InputSplit[0]);
    } else {
      Statement statement = null;
      List<InputSplit> splits = Lists.newArrayList();
      try {
        if (connection == null) {
          openConnection();
        }
        statement = connection.createStatement();
        String query = "SELECT __part_id as partitionId, count(*) as numrows FROM " + tempTableName
            + " GROUP BY __part_id";
        ResultSet results;
        results = statement.executeQuery(query);
        while (results.next()) {
          int partitionId = results.getInt("partitionId");
          long numrows = results.getLong("numrows");
          splits.add(new TeradataInputSplit(partitionId, numrows));
        }
        statement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      } finally {
        closeConnection();
      }
      return splits.toArray(new InputSplit[0]);
    }
  }

  class TeradataDBRecordReader extends DBInputFormat.DBRecordReader {

    protected TeradataDBRecordReader(DBInputSplit split, Class inputClass, JobConf job) throws SQLException,
        IOException {
      super(split, inputClass, job);
    }

    /** Returns the query for selecting the records from an Teradata DB. */
    protected String getSelectQuery() {
      String inputQuery = dbConf.getInputQuery();
      if (inputQuery == null) {
        throw new RuntimeException("Expected input query to be set by scheme");
      }
      StringBuilder query = new StringBuilder(inputQuery);
      if (dbConf.getMaxConcurrentReadsNum() > 1) {
        try {
          TeradataInputSplit tsplit = (TeradataInputSplit) split;
          if (tsplit.getLength() > 0 && tsplit.getPartitionId() >= 0) {
            query.append(" WHERE __part_id = ").append(tsplit.getPartitionId());
          }
        } catch (IOException ex) {
          // ignore, will not throw.
        }
      }
      return query.toString();
    }
  }

  @Override
  protected RecordReader<LongWritable, DBWritable> getRecordReaderInternal(
      cascading.jdbc.db.DBInputFormat.DBInputSplit split, Class inputClass, JobConf job) throws SQLException,
    IOException {
    return new TeradataDBRecordReader(split, inputClass, job);
  }
}
