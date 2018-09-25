/**
 * partitioner for the partitioning the intermediate data.
 */
package com.demo.bigdata.bid;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.demo.bigdata.bid.model.BidInfoWritable;

/**
 * @author Shiva_Donkena deriving the partition with the hashcode of the osType
 *         from the bidInfoWritable.
 */
public class HighBidCountPartitioner extends Partitioner<BidInfoWritable, LongWritable> {

	@Override
	public int getPartition(BidInfoWritable key, LongWritable value, int numPartitions) {
		return (key.getOsType().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
