/**
 * Combiner to calculate the sum of ipbytes and the count of the Ip.
 */
package com.demo.bigdata.mapreduce;

import java.io.IOException;

import com.demo.bigdata.writable.IpBytesDataWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shiva_Donkena To calculate the sum of ipbytes and the count of the
 *         occurrence Ip
 */
public class IpCombiner extends Reducer<Text, IpBytesDataWritable, Text, IpBytesDataWritable> {
	private static IpBytesDataWritable ipBytesAndCountWritable;

	@Override
	protected void setup(Context context) {
		ipBytesAndCountWritable = new IpBytesDataWritable();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable,
	 * org.apache.hadoop.mapreduce.Reducer.Context) Reducing the key and value
	 * intermediate values.
	 */
	@Override
	public void reduce(Text key, Iterable<IpBytesDataWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		int count = 0;
		// to find the count of the ip and sum of the bytes
		for (IpBytesDataWritable sumCounter : values) {
			sum += sumCounter.getBytes().get();
			count += sumCounter.getIpValueCount().get();
		}
		// setting the bytes and the ipvalue count.
		ipBytesAndCountWritable.setBytesSum(sum);
		ipBytesAndCountWritable.setIpValue(count);
		context.write(key, ipBytesAndCountWritable);
	}

}
