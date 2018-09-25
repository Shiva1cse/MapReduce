/**
 *Test cases for checking the mapper class.
 */
package com.demo.bigdata.bid;
import com.demo.bigdata.bid.model.BidInfoWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Shiva_Donkena
 *
 */
public class HighBidCountMapperTest {
	private MapDriver<LongWritable, Text, BidInfoWritable, LongWritable> mapDriver;

	/**
	 * Mocking the mapper
	 */
	@Before
	public void setUp() {
		HighBidCountMapper mapper = new HighBidCountMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}
	
	/**
	 * Tests that mapper correct count of high bids
	 */
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(LogsConstant.FIRST_LOG));
		mapDriver.withInput(new LongWritable(), new Text(LogsConstant.SECOND_LOG));
		mapDriver.withInput(new LongWritable(), new Text(LogsConstant.THRID_LOG));
		mapDriver.withOutput(new BidInfoWritable("unknown", "Android",202), new LongWritable(1));
		mapDriver.withOutput(new BidInfoWritable("unknown", "Android",220), new LongWritable(1));
		mapDriver.runTest();
	}
}