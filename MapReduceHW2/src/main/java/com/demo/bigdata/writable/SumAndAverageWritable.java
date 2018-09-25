/**
 * To set the sum and the average if the ipbytesF.
 */
package com.demo.bigdata.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author Shiva_Donkena Bean for the sum and average of the bytes.
 */
public class SumAndAverageWritable implements Writable {
	private LongWritable sumOfBytes;
	private DoubleWritable averageOfBytes;

	public LongWritable getSumOfBytes() {
		return sumOfBytes;
	}

	public void setSumOfBytes(long sumOfBytes) {
		this.sumOfBytes = new LongWritable(sumOfBytes);
	}

	public DoubleWritable getAverageOfBytes() {
		return averageOfBytes;
	}

	public void setAverageOfBytes(double averageOfBytes) {
		this.averageOfBytes = new DoubleWritable(averageOfBytes);
	}

	public SumAndAverageWritable() {
		sumOfBytes = new LongWritable(0);
		averageOfBytes = new DoubleWritable(0);
	}

	public SumAndAverageWritable(long SumOfBytes, double averageOfBytes) {
		this.sumOfBytes = new LongWritable(SumOfBytes);
		this.averageOfBytes = new DoubleWritable(averageOfBytes);
	}

	/**
	 * To compares the objects with its properties.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SumAndAverageWritable other = (SumAndAverageWritable) obj;
		if (averageOfBytes == null) {
			if (other.averageOfBytes != null)
				return false;
		} else if (!averageOfBytes.equals(other.averageOfBytes))
			return false;
		if (sumOfBytes == null) {
			if (other.sumOfBytes != null)
				return false;
		} else if (!sumOfBytes.equals(other.sumOfBytes))
			return false;
		return true;
	}

	public void write(DataOutput out) throws IOException {
		sumOfBytes.write(out);
		averageOfBytes.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		sumOfBytes.readFields(in);
		averageOfBytes.readFields(in);
	}

	/**
	 * To print the data in the CSV format.
	 */
	@Override
	public String toString() {
		return "," + sumOfBytes + "," + averageOfBytes;
	}

}
