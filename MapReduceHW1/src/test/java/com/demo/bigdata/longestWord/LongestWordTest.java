/**
 *Test cases for the map and reduce phase. 
 */
package com.demo.bigdata.longestWord;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.demo.bigdata.longestword.LongestWordMapper;
import com.demo.bigdata.longestword.LongestWordReducer;

/**
 * @author Shiva_Donkena To check the output of the reduce phase.
 */
public class LongestWordTest {
	private MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReducedriver;

	/**
	 * Mocking the mapper and reducer.
	 */
	@Before
	public void setUp() {
		LongestWordMapper longestWordMapper = new LongestWordMapper();
		LongestWordReducer longestWordReducer = new LongestWordReducer();
		mapReducedriver = MapReduceDriver.newMapReduceDriver(longestWordMapper, longestWordReducer);
	}

	/**
	 * To check the output of the map and reduce phase and with certain inputs.
	 * @throws IOException 
	 */
	@Test
	public void testMapReduceResult() throws IOException {
		mapReducedriver.withInput(new LongWritable(1), new Text("Shoppers?????? who scroll down to the detailed product descriptions??? are clearly interested in learning"
				+ " more about a product. If you make the product (description) too short, they may decide to look elsewhere for information###."
				+ " Still, overly long (descriptions) can result in information overload."));
		mapReducedriver.withOutput(new IntWritable(12), new Text("descriptions"));
		mapReducedriver.runTest();
	}
}
