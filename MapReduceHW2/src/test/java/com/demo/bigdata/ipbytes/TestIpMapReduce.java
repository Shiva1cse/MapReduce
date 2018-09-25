/**
 * To test the map and the reduce phase combined.
 */
package com.demo.bigdata.ipbytes;

import java.io.IOException;

import com.demo.bigdata.mapreduce.IpMapper;
import com.demo.bigdata.mapreduce.IpReducer;
import com.demo.bigdata.writable.IpBytesDataWritable;
import com.demo.bigdata.writable.SumAndAverageWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Shiva_Donkena mocking the mapreduce
 *
 */
public class TestIpMapReduce {
	private MapReduceDriver<LongWritable, Text, Text, IpBytesDataWritable, Text, SumAndAverageWritable> driver;
	IpMapper mapper;
	IpReducer ipReducer;

	/**
	 * Mocking the mapreduce.
	 */
	@Before
	public void setUp() {
		driver = new MapReduceDriver<>();
		mapper = new IpMapper();
		ipReducer = new IpReducer();
		driver = MapReduceDriver.newMapReduceDriver(mapper, ipReducer);
	}

	/**
	 * To test the map and reduce phase.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapReduce() throws IOException {
		driver.withInput(new LongWritable(), new Text(IpConstant.TEST_MAP_INPUT1));
		driver.withInput(new LongWritable(), new Text(IpConstant.TEST_MAP_INPUT2));
		driver.withOutput(new Text("ip1"), new SumAndAverageWritable(46272, 23136.0));
		driver.runTest();
	}
}
