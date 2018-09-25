/**
 * To find the Ipbytes sum and the average and then compress the data.
 */
package com.demo.bigdata.ipbytes;

import com.demo.bigdata.mapreduce.IpCombiner;
import com.demo.bigdata.mapreduce.IpMapper;
import com.demo.bigdata.mapreduce.IpReducer;
import com.demo.bigdata.writable.IpBytesDataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * @author Shiva_Donkena CalculateIpBytes sum and the average for each ip.
 */
public class CalculateIpBytes {

	/**
	 * Logger Configuration.
	 */
	private final static Logger LOGGER = Logger.getLogger(CalculateIpBytes.class);

	/**
	 * @param args
	 *            command line arguments.
	 * @throws Exception
	 *             {@link Exception}
	 */
	public static void main(final String[] args) throws Exception {
		LOGGER.info("Started of the main method");
		if (args.length != 3) {
			LOGGER.info(
					"Usage - give the three arguments 1) input directory 2)output directory 3)Type of output format");
			System.exit(0);
		}
		// configuration of the job.
		Configuration conf = new Configuration();
		// Creating the job.
		Job job = Job.getInstance(conf, "IpBytesSum");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IpBytesDataWritable.class);
		job.setJarByClass(CalculateIpBytes.class);
		
		job.setMapperClass(IpMapper.class);
		job.setCombinerClass(IpCombiner.class);
		job.setReducerClass(IpReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Input paths for the accessing the data.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));

		if (args[2].equalsIgnoreCase("compress")) {
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// For compressing the data output.Format
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
			conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

		} else if (args[2].equalsIgnoreCase("csv")) {
			TextOutputFormat.setOutputPath(job, new Path(args[1]));
		} else {
			LOGGER.info("Enter the type of output format as compress or csv the 3rd command line argument!!");
			System.exit(0);
		}

		int result = job.waitForCompletion(true) ? 0 : 1;
		CounterGroup group = job.getCounters().getGroup("Browsers");
		System.out.println("Total browsers in the log file= " + group.size());
		// list of all the counters for browsers.
		for (Counter counter : group) {
			System.out.println(counter.getName() + ": " + counter.getValue());
		}

		if (result == 0) {
			LOGGER.info("Job was successful");
		} else {
			LOGGER.info("Job was not successful");
		}
		LOGGER.info("End of the main method");
	}
}
