/**
 *Test the reducer. 
 */
package com.demo.bigdata.ipbytes;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.demo.bigdata.mapreduce.IpReducer;
import com.demo.bigdata.writable.IpBytesDataWritable;
import com.demo.bigdata.writable.SumAndAverageWritable;

/**
 * @author Shiva_Donkena
 *
 */
public class IpReducerTest {
	ReduceDriver<Text, IpBytesDataWritable, Text, SumAndAverageWritable> reduceDriver;

	/**
	 * @throws Exception
	 *             {@link Exception} To mock the Reduce data
	 */
	@Before
	public void setUp() throws Exception {
		IpReducer reducer = new IpReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	/**
	 * checking the reduce phase with the sum and average.
	 * 
	 * @throws IOException
	 *             {{@link IOException}
	 */
	@Test
	public void sampleTest() throws IOException {
		reduceDriver.withInput(new Text("ip7"), Arrays.asList(new IpBytesDataWritable(3, 300)));
		reduceDriver.withOutput(new Pair<>(new Text("ip7"), new SumAndAverageWritable(300, 100.0)));
		reduceDriver.runTest();
	}
}
