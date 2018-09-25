/**
 * To main class for finding the highest bid count.
 */
package com.demo.bigdata.driver;

import com.demo.bigdata.bid.HighBidCountCombiner;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.net.URISyntaxException;

import com.demo.bigdata.bid.HighBidCountMapper;
import com.demo.bigdata.bid.HighBidCountPartitioner;
import com.demo.bigdata.bid.HighBidCountReducer;
import com.demo.bigdata.bid.model.BidInfoWritable;
import java.io.IOException;

/**
 * @author Shiva_Donkena
 *
 */
public class HighBidCount extends Configured implements Tool {
	public enum MyCounters {
		ErrorCounters
	}

	private static Logger logger = Logger.getLogger(HighBidCount.class);

	/**
	 * @param args
	 *            Passing the input file, output file and the cache file as the
	 *            command line Arguments to the main method.
	 * 
	 */
	public static void main(final String[] args) throws Exception {
		logger.info("Main method started!!");
		int res = ToolRunner.run(new Configuration(), new HighBidCount(), args);
		logger.info("end of main method!");
		System.exit(res);
	}

	/**
	 * 
	 * 
	 * @return int result status of job.
	 * @throws URISyntaxException
	 *             {@link URISyntaxException}
	 */
	@Override
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		Configuration conf = getConf();
		if (args.length < 2) {
			logger.info("usage hadoop jar /input /output /cache");
			return 2;
		}

		Job job = Job.getInstance(conf, "logs stats analizing");
		job.setJarByClass(HighBidCount.class);
		job.setMapperClass(HighBidCountMapper.class);
		job.setCombinerClass(HighBidCountCombiner.class);
		job.setReducerClass(HighBidCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(BidInfoWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(2);
		// partitioner class.
		job.setPartitionerClass(HighBidCountPartitioner.class);

		// Cache File
		job.addCacheFile(new Path(args[2]).toUri());
		// input file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// output file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
