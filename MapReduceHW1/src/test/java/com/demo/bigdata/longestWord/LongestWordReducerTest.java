/**
 * Test cases for the reducer.
 */
package com.demo.bigdata.longestWord;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.demo.bigdata.longestword.LongestWordReducer;

/**
 * @author Shiva_Donkena To test the reducer phase with the possible input text
 *         for finding the same length words.
 */
public class LongestWordReducerTest {
	private ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;

	/**
	 * Mocking the reducer.
	 */
	@Before
	public void setUp() {
		LongestWordReducer longestWordReducer = new LongestWordReducer();
		reduceDriver = ReduceDriver.newReduceDriver(longestWordReducer);

	}

	/**
	 * @throws IOException
	 *             To check the same length words as a set.
	 */
	@Test
	public void reducerTest() throws IOException {
		reduceDriver.withInput(new IntWritable(5), Arrays.asList(new Text("word1"), new Text("word2")));
		reduceDriver.withInput(new IntWritable(9), Arrays.asList(new Text("mapreduce1"), new Text("mapreduce2")));
		reduceDriver.withOutput(new IntWritable(9), new Text("mapreduce1"));
		reduceDriver.withOutput(new IntWritable(9), new Text("mapreduce2"));
		reduceDriver.runTest();
	}
}
