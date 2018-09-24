/**
 * Test cases for the mapper.
 */
package com.demo.bigdata.longestWord;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.demo.bigdata.longestword.LongestWordMapper;

/**
 * @author Shiva_Donkena
 *
 */
public class LongestWordMapperTest {
	private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

	/**
	 * Mocking the mapper.
	 */
	@Before
	public void setUp() {
		LongestWordMapper longestWordMapper = new LongestWordMapper();
		mapDriver = new MapDriver<LongWritable, Text, IntWritable, Text>();
		mapDriver.setMapper(longestWordMapper);
	}

	/**
	 * @throws IOException
	 *             {@link IOException} Checking the length of the words from the
	 *             mapper.
	 */
	@Test
	public void mapperTest() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("Hi I am Good. Hello (yyyyy) ?Rocke#"));
		mapDriver.withOutput(new IntWritable(5), new Text("Hello"));
		mapDriver.withOutput(new IntWritable(5), new Text("Rocke"));
		mapDriver.withOutput(new IntWritable(5), new Text("yyyyy"));
		mapDriver.runTest();
	}
}
