/**
 * To get the sum and the average of the ip bytes.
 */
package com.demo.bigdata.mapreduce;

import java.io.IOException;

import com.demo.bigdata.writable.IpBytesDataWritable;
import com.demo.bigdata.writable.SumAndAverageWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shiva_Donkena Reduce Phase
 */
public class IpReducer extends Reducer<Text, IpBytesDataWritable, Text, SumAndAverageWritable> {
	private SumAndAverageWritable sumAverageWritable;

	@Override
	protected void setup(Context context) {
		sumAverageWritable = new SumAndAverageWritable();
	}

	/**
	 * To reduce the intermediate key value pairs for calculating the sum and then
	 * average if the ipbytes.
	 */
	@Override
	public void reduce(Text key, Iterable<IpBytesDataWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		int count = 0;
		double average = 0;
		// to find the count of the ip and sum of the bytes
		for (IpBytesDataWritable sumCounter : values) {
			sum += sumCounter.getBytes().get();
			count += sumCounter.getIpValueCount().get();
		}
		average = (double) sum / count;
		sumAverageWritable.setSumOfBytes(sum);
		sumAverageWritable.setAverageOfBytes(average);
		// output as the CSV format
		context.write(key, sumAverageWritable);
	}
}
