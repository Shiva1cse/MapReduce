/**
 * To get the ip and the total bytes for ip.
 */
package com.demo.bigdata.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author Shiva_Donkena To set and get the bytes and ip value count.
 */
public class IpBytesDataWritable implements Writable {

	private IntWritable ipValueCount;
	private LongWritable ipBytes;

	public IpBytesDataWritable() {
		ipValueCount = new IntWritable(0);
		ipBytes = new LongWritable(0);
	}

	/**
	 * To assign values of ipValuecount and ipbytes value.
	 * 
	 * @param ipValue
	 * @param ipBytes
	 */
	public IpBytesDataWritable(int ipValue, int ipBytes) {
		this.ipValueCount = new IntWritable(ipValue);
		this.ipBytes = new LongWritable(ipBytes);
	}

	public IntWritable getIpValueCount() {
		return ipValueCount;
	}

	/**
	 * @return ipbytes.
	 */
	public LongWritable getBytes() {
		return ipBytes;
	}

	public void setIpValue(int intWritable) {
		this.ipValueCount = new IntWritable(intWritable);
	}

	public void setBytesSum(long ipBytesum) {
		this.ipBytes = new LongWritable(ipBytesum);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IpBytesDataWritable other = (IpBytesDataWritable) obj;
		if (ipBytes == null) {
			if (other.ipBytes != null)
				return false;
		} else if (!ipBytes.equals(other.ipBytes)) {
			return false;
		}
		if (ipValueCount == null) {
			if (other.ipValueCount != null)
				return false;
		} else if (!ipValueCount.equals(other.ipValueCount)) {
			return false;
		}
		return true;
	}

	public void readFields(DataInput in) throws IOException {
		ipValueCount.readFields(in);
		ipBytes.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		ipValueCount.write(out);
		ipBytes.write(out);
	}
}
