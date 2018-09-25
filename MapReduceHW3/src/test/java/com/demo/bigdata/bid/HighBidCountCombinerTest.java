/**
 * Test cases for checking the working of the combiner.
 */
package com.demo.bigdata.bid;

import com.demo.bigdata.bid.model.BidInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Shiva_Donkena Combiner output testing. Tests the combiner for the
 *         count and the osType based on the city name
 */
public class HighBidCountCombinerTest {

	private ReduceDriver<BidInfoWritable, LongWritable, BidInfoWritable, LongWritable> reduceDriver;

	/**
	 * Mocking the combiner
	 */
	@Before
	public void setup() {
		HighBidCountCombiner reducer = new HighBidCountCombiner();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	/**
	 * Tests the combiner for the count and the osType based on the city id.
	 */
	@Test
	public void testBidReducer() throws IOException {

		reduceDriver.withInput(new BidInfoWritable("unknown", "Android", 101), Arrays.asList(new LongWritable(1),
				new LongWritable(1), new LongWritable(1), new LongWritable(1), new LongWritable(1)));

		reduceDriver.withAllOutput(
				Arrays.asList(new Pair<>(new BidInfoWritable("unknown", "Android", 101), new LongWritable(5))));
		reduceDriver.runTest();
	}
}
