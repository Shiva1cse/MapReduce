/**
 * To test the working of the overall mapreduce code.
 */
package com.demo.bigdata.bid;

import com.demo.bigdata.bid.model.BidInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Shiva_Donkena test cases for the mapreduce.
 */
public class HighBidCountTest {
	private MapReduceDriver<LongWritable, Text, BidInfoWritable, LongWritable, Text, LongWritable> mapReduceDriver;

	/**
	 * Mocking mapreduce task
	 */
	@Before
	public void setup() {
		HighBidCountMapper mapper = new HighBidCountMapper();
		HighBidCountReducer reducer = new HighBidCountReducer();
		HighBidCountCombiner combiner = new HighBidCountCombiner();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
	}

	/**
	 * Tests that full cycle will find bids stats
	 */
	@Test
	public void testBidMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(LogsConstant.FIRST_LOG));
		mapReduceDriver.withInput(new LongWritable(), new Text(LogsConstant.SECOND_LOG));
		mapReduceDriver.withInput(new LongWritable(), new Text(LogsConstant.THRID_LOG));
		mapReduceDriver.withAllOutput(Arrays.asList(new Pair<>(new Text("unknown"), new LongWritable(2))));
		mapReduceDriver.runTest();
	}
}
