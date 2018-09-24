/**
 * Reducer phase finding the longest word and
 * appending all the words with similar lengths.
 */
package com.demo.bigdata.longestword;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shiva_Donkena To find the longest word form the list of the input
 *         from the mapper phase.
 */
public class LongestWordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	private Set<String> words;
	private IntWritable outputKey;
	private Text outputValue;
	private int maxLength = 0;

	@Override
	protected void setup(Context context) {
		words = new HashSet<>();
		outputKey = new IntWritable();
		outputValue = new Text();
	}

	/**
	 * To find the max length words form the intermediate key value pair from the
	 * mapper.
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (key.get() > maxLength) {
			maxLength = key.get();
			words.clear();
			for (Text value : values) {
				words.add(value.toString());
			}
		}
	}

	/**
	 * To clean up and the write the output key with longest length and the set of
	 * the longest words with same length.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputKey.set(maxLength);
		for (String word : words) {
			outputValue.set(word);
			context.write(outputKey, outputValue);
		}
	}
}
