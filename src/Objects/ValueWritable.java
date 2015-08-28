package Objects;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class ValueWritable implements WritableComparable<ValueWritable> {

	private double value1 = 0;
	private double value2 = 0;
	private double value3 = 0;
	private double total = 0;
	
	public ValueWritable() {
		value1 = 0;
		value2 = 0;
		value3 = 0;
	}
	
	public ValueWritable(double val1, double total) {
		this.value1 = val1;
		this.total = total;
		this.value2 = 0;
		this.value3 = 0;
	}
	
	public ValueWritable(double val1, double val2, double total) {
		this.value1 = val1;
		this.value2 = val2;
		this.value3 = 0;
		this.total = total;
	}
	
	public ValueWritable(double val1, double val2, double val3, double total) {
		this.value1 = val1;
		this.value2 = val2;
		this.value3 = val3;
		this.total = total;
	}
	
	public double getVal1() {
		return value1;
	}
	
	public double getVal2() {
		return value2;
	}
	
	public double getVal3() {
		return value3;
	}
	
	public double getTotal() {
		return total;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		value1 = arg0.readDouble();
		value2 = arg0.readDouble();
		value3 = arg0.readDouble();
		total = arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(value1);
		arg0.writeDouble(value2);
		arg0.writeDouble(value3);
		arg0.writeDouble(total);
	}

	@Override
	public int compareTo(ValueWritable o) {
		if (Double.compare(this.getVal1(), o.getVal1())== 0) {
			return Double.compare(this.getVal2(), o.getVal2());
		} else {
			return Double.compare(this.getVal1(), o.getVal1());
		}
	}

}
