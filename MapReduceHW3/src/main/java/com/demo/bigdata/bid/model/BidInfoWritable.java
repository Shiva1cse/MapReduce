/**
 * Writables for the count and osType.
 */
package com.demo.bigdata.bid.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Shiva_Donkena To get and set the cityName and osType of the different
 *         logs data.
 */
public class BidInfoWritable implements WritableComparable<BidInfoWritable> {

	private IntWritable cityId;
	private Text cityName;
	private Text osType;

	/**
	 * Writes data.
	 * 
	 * @param out
	 *            output
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		cityName.write(out);
		osType.write(out);
		cityId.write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput) input to
	 * writable
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		cityName.readFields(in);
		osType.readFields(in);
		cityId.readFields(in);
	}

	/**
	 * @param cityName
	 * @param osType
	 */
	public BidInfoWritable(String cityName, String osType, int cityId) {
		this.cityName = new Text(cityName);
		this.osType = new Text(osType);
		this.cityId = new IntWritable(cityId);
	}

	public BidInfoWritable() {
		cityName = new Text();
		osType = new Text();
		cityId = new IntWritable();
	}

	/**
	 * @return cityname
	 */
	public Text getCityName() {
		return cityName;
	}

	/**
	 * @param cityname
	 */
	public void setCityName(String cityName) {
		this.cityName = new Text(cityName);
	}

	/**
	 * @return osType
	 */
	public Text getOsType() {
		return osType;
	}

	/**
	 * @param osType
	 */
	public void setOsType(String osType) {
		this.osType = new Text(osType);
	}

	public IntWritable getCityId() {
		return cityId;
	}

	public void setCityId(int cityId) {
		this.cityId = new IntWritable(cityId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object) for comparing the objects.
	 */

	@Override
	public int compareTo(BidInfoWritable o) {
		int result = this.cityName.compareTo(o.cityName);
		return result;

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cityId == null) ? 0 : cityId.hashCode());
		result = prime * result + ((cityName == null) ? 0 : cityName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BidInfoWritable other = (BidInfoWritable) obj;
		if (cityId == null) {
			if (other.cityId != null)
				return false;
		} else if (!cityId.equals(other.cityId))
			return false;
		if (cityName == null) {
			if (other.cityName != null)
				return false;
		} else if (!cityName.equals(other.cityName))
			return false;
		return true;
	}

}
