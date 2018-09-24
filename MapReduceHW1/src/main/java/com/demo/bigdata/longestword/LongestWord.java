/**
 * To find the longest word from the input files.
 */
package com.demo.bigdata.longestword;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author Shiva_Donkena To find the longest word from all the input files
 *         provided.
 */

public class LongestWord {

	/**
	 * @param args
	 *            command line args
	 * @throws InterruptedException
	 *             {@link InterruptedException}
	 * @throws ClassNotFoundException
	 *             {@link ClassNotFoundException} Main method to take the command
	 *             line arguments and starts up the map, reducer, combiner phase.
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage - give the two arguments 1) input directory 2)output directory");
			System.exit(0);
		}
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "find longest words");
		job.setJarByClass(LongestWord.class);

		job.setMapperClass(LongestWordMapper.class);
		job.setReducerClass(LongestWordReducer.class);
		job.setCombinerClass(LongestWordReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int result = job.waitForCompletion(true) ? 0 : 1;

		if (result == 0) {
			System.out.println("Job was successful");
		} else {
			System.out.println("Job was not successful");
		}
		System.exit(0);

	}
}
