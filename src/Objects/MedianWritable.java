package Objects;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MedianWritable implements WritableComparable<MedianWritable> {

	private String range = "";
	private int count = 0;

	public MedianWritable() {
		range = "";
		count = 0;
	}
	
	public MedianWritable(MedianWritable val) {
		this.range = val.getRange();
		this.count = val.getCount();
	}

	public MedianWritable(String range, int count) {
		this.range = range;
		this.count = count;
	}

	public String getRange() {
		return range;
	}

	public int getCount() {
		return count;
	}
	
	public void addToCount(int value) {
		count += value;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		range = arg0.readUTF();
		count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(range);
		arg0.writeInt(count);
	}

	@Override
	public int compareTo(MedianWritable o) {
		return Integer.compare(this.getCount(), o.getCount());
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof MedianWritable) {
			return equalsObject((MedianWritable) o);
		} else {
			return false;
		}
	}
	
	private boolean equalsObject(MedianWritable o) {
		return this.range.trim().equalsIgnoreCase(o.getRange().trim());
	}
}
