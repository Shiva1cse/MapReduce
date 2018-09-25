/**
 * Combiner for the bid count.
 */
package com.demo.bigdata.bid;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import com.demo.bigdata.bid.model.BidInfoWritable;
import java.io.IOException;

/**
 * @author Shiva_Donkena For counting the count of the osType of the each
 *         cityId.
 */
public class HighBidCountCombiner extends Reducer<BidInfoWritable, LongWritable, BidInfoWritable, LongWritable> {

	private LongWritable impressionsCount;

	@Override
	protected void setup(Context context) {
		impressionsCount = new LongWritable();
	}

	/**
	 * calculating the osType counts.
	 *
	 * @param bidWritable
	 *            this is the bidwritable for the combiner has cityId, osType,
	 *            cityName.
	 * @param context
	 *            its a context of the reducer.F
	 * 
	 */
	@Override
	protected void reduce(BidInfoWritable key, Iterable<LongWritable> value, Context context)
			throws IOException, InterruptedException {
		int totalImpressions = 0;
		// for incrementing the count value
		for (LongWritable count : value) {
			totalImpressions += count.get();
		}
		impressionsCount.set(totalImpressions);
		context.write(key, impressionsCount);
	}
}