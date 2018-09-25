/**
 * Applying the reducer to the intermediate key value pair.
 */
package com.demo.bigdata.bid;

import com.demo.bigdata.bid.model.BidInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Shiva_Donkena To take the intermediate key value pairs and then
 *         fetches the city names and then counts the city counts.
 */
public class HighBidCountReducer extends Reducer<BidInfoWritable, LongWritable, Text, LongWritable> {

	/**
	 * count of the osType.
	 */
	private LongWritable impressionsCount;

	/**
	 * To store the exceptions in the logger.
	 */
	Logger logger;

	/**
	 * Setup method for initalising the count, logger.
	 * 
	 * @param context
	 *            reducer context
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		impressionsCount = new LongWritable();
		logger = Logger.getLogger(HighBidCountReducer.class);
	}

	/**
	 * Reducing method, that calculates final count of bids.
	 * 
	 * @param key
	 *            contains the cityName and the osType
	 * @param bidWritable
	 *            count value of the bid's
	 * @param context
	 *            context.
	 */
	@Override
	protected void reduce(BidInfoWritable key, Iterable<LongWritable> bidWritable, Context context)
			throws IOException, InterruptedException {
		int totalImpresssions = 0;
		for (LongWritable currentBidInfo : bidWritable) {
			totalImpresssions += currentBidInfo.get();
		}
		impressionsCount.set(totalImpresssions);
		context.write(new Text(key.getCityName()), impressionsCount);
	}
}