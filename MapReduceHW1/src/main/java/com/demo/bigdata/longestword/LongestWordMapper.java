/**
 * Mapper class to find the length of the each word
 * in file and then add the word and its length to the context.
 */
package com.demo.bigdata.longestword;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shiva_Donkena Class for finding the set of the longest words in a
 *         line.
 */
public class LongestWordMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private Text longestWords;
	private IntWritable LongestLength;

	private int maxLength = 0;

	/**
	 * To collect all the longest length words.
	 */
	private Set<String> maxLengthWords;

	/**
	 * Initalising the Variables.
	 */
	@Override
	protected void setup(Context context) {
		maxLengthWords = new HashSet<>();
		longestWords = new Text();
		LongestLength = new IntWritable(0);
	}

	/**
	 * To split the line into words and then collecting the longest length word into
	 * the set by removing the duplicates.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for (String word : words) {
			
			// to remove all the special characters from the word.
			String wordWithoutSpecialCharacters = word.replaceAll("[^a-zA-Z0-9\\s+]", "").trim();

			// to check that word has length greater or equals then the previous words
			// length
			if (wordWithoutSpecialCharacters.length() >= maxLength) {

				// if we find the new longest length the clear the set which has previous
				// longest words.
				if (wordWithoutSpecialCharacters.length() > maxLength) {
					maxLength = wordWithoutSpecialCharacters.length();
					maxLengthWords.clear();
				}
				maxLengthWords.add(wordWithoutSpecialCharacters);
			}
		}
	}

	/**
	 * cleaning up the job on the job completion. writing the words with longest
	 * length to the context.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (maxLengthWords.isEmpty()) {
			return;
		}
		for (String word : maxLengthWords) {
			longestWords.set(word);
			LongestLength.set(word.length());
			context.write(LongestLength, longestWords);
		}
	}

}
