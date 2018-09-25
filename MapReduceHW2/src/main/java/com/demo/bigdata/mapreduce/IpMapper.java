/**
 * To get the Ip value and the counting the Ip values and then get the bytes size.
 */
package com.demo.bigdata.mapreduce;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import com.demo.bigdata.writable.IpBytesDataWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.demo.bigdata.exception.PatternNotMatchedException;
import com.demo.bigdata.parse.LogDataParser;

import eu.bitwalker.useragentutils.UserAgent;

/**
 * @author Shiva_Donkena. To get and set the ip value, its count and the sum of
 *         the bytes.
 */
public class IpMapper extends Mapper<LongWritable, Text, Text, IpBytesDataWritable> {
	private IpBytesDataWritable ipBytesSumCountWritable;
	private Text ipName;
	private Logger LOGGER;

	@Override
	protected void setup(Context context) {
		ipBytesSumCountWritable = new IpBytesDataWritable();
		ipName = new Text();
		LOGGER = Logger.getLogger(IpMapper.class.getName());
	}

	/**
	 * To read the data from the files or file and then create the key value pairs.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// UserAgent parsing for finding the browsers used.
		UserAgent userAgent = UserAgent.parseUserAgentString(value.toString());
		context.getCounter("Browsers", userAgent.getBrowser().getName()).increment(1);
		LogDataParser parsedIp = null;
		try {
			parsedIp = LogDataParser.praseLogRequestLine(value.toString());
			// get the Ip from the parsed data.
			ipName.set(parsedIp.getIpAddress());
			ipBytesSumCountWritable.setIpValue(1);
			ipBytesSumCountWritable.setBytesSum(parsedIp.getByteSize());
			context.write(ipName, ipBytesSumCountWritable);
		} catch (PatternSyntaxException | NumberFormatException | PatternNotMatchedException e) {
			// cannot parse the line.
			LOGGER.info("The exception is " + e.toString());
		}
	}
}