/**
 * Test cases for checking the reducer by mocking the data. 
 */
package com.demo.bigdata.bid;

import com.demo.bigdata.bid.model.BidInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author Shiva_Donkena
 *
 */
public class HighBidCountReducerTest {
	private ReduceDriver<BidInfoWritable, LongWritable, Text, LongWritable> reduceDriver;

	/**
	 * Mocking the reducer
	 */
	@Before
	public void setup() throws URISyntaxException {
		HighBidCountReducer reducer = new HighBidCountReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	/**
	 * Tests that combiner correctly count total high bids
	 */
	@Test
	public void testBidReducer() throws IOException {
		reduceDriver.withInput(new BidInfoWritable("unknown", "Android", 100),
				Arrays.asList(new LongWritable(3), new LongWritable(1)));
		reduceDriver.withAllOutput(Arrays.asList(new Pair<>(new Text("unknown"), new LongWritable(4))));
		reduceDriver.runTest();
	}
}
