package cascading.jdbc.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import cascading.jdbc.db.DBInputFormat.DBInputSplit;

public class TeradataInputSplit extends DBInputSplit {
  private int partitionId = -1;
  private long length = -1;

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  /** Default Constructor */
  public TeradataInputSplit() {
  }

  public TeradataInputSplit(int partitionId, long length) {
    this.partitionId = partitionId;
    this.length = length;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    output.writeLong(length);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    partitionId = input.readInt();
    length = input.readLong();
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  public int getPartitionId() {
    return partitionId;
  }
}
