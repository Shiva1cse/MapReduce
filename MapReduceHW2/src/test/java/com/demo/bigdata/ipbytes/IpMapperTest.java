/**
 * Test cases for the Ip mapper.
 */
package com.demo.bigdata.ipbytes;

import java.io.IOException;

import com.demo.bigdata.mapreduce.IpMapper;
import com.demo.bigdata.writable.IpBytesDataWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Shiva_Donkena
 *
 */
public class IpMapperTest {
	MapDriver<LongWritable, Text, Text, IpBytesDataWritable> mapDriver;
	Text ipAddress;

	/**
	 * @throws Exception
	 *             {@link Exception} Create the Mock for the mapper.
	 */
	@Before
	public void setUp() throws Exception {
		IpMapper mapper = new IpMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		ipAddress = new Text("ip1");
	}

	/**
	 * for testing the functionality of with input data and checking output.
	 * 
	 * @throws IOException
	 *             {{@link IOException}
	 */
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(IpConstant.TEST_MAP_INPUT1));
		mapDriver.withOutput(new Text("ip1"), new IpBytesDataWritable(1, 40028));
		mapDriver.withInput(new LongWritable(), new Text(IpConstant.TEST_MAP_INPUT2));
		mapDriver.withOutput(new Text("ip1"), new IpBytesDataWritable(1, 6244));
		mapDriver.runTest();
	}
}
