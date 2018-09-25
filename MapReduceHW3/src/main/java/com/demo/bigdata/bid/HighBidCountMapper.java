/**
 * Mapper class for finding bid value greater then 250
 * and then get osType and city id.
 */
package com.demo.bigdata.bid;

import com.demo.bigdata.bid.model.BidInfoWritable;
import com.demo.bigdata.driver.HighBidCount;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.log4j.Logger;
import eu.bitwalker.useragentutils.UserAgent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Shiva_Donkena Mapper class for fining the bid value and then get the
 *         osType and the city name and set the count.
 */
@SuppressWarnings("deprecation")
public class HighBidCountMapper extends Mapper<LongWritable, Text, BidInfoWritable, LongWritable> {
	private static final int USER_AGENT = 4;
	private static final int CITY_ID = 7;
	private static final int BIDDING_PRICE = 19;
	private static final int THRESOLD_BID_VALUE = 250;
	private BidInfoWritable bidContent;
	private static Logger logger = Logger.getLogger(HighBidCountMapper.class);
	private Map<Integer, String> cities;
	private LongWritable count;

	@Override
	protected void setup(Context context) throws IOException {
		bidContent = new BidInfoWritable();
		cities = new HashMap<>();
		count = new LongWritable(1);
		try {
			readCacheFilesData(context);
		} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
			context.getCounter(HighBidCount.MyCounters.ErrorCounters).increment(1);
			logger.info(e.getMessage());
		}
	}

	/**
	 * Method for reading mapping file and then putting the id and the city name
	 * into the map
	 * 
	 * @param uri
	 *            file uri for path of cache file.
	 * @throws {@link
	 *             IOException}
	 */
	private void readCacheFilesData(Context context)
			throws IOException, NumberFormatException, ArrayIndexOutOfBoundsException {
		/**
		 * To get all the uri's form the distributed cache.
		 */
		URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
		if (uris != null) {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			/**
			 * fetching all the URI'S
			 */
			for (int i = 0; i < uris.length; i++) {
				Path path = new Path(uris[i].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
				String mappingLine;
				while ((mappingLine = reader.readLine()) != null) {
					String[] mapping = mappingLine.split("\t");
					cities.put(Integer.parseInt(mapping[0]), mapping[1]);
				}
			}
		}
	}

	/**
	 * Mapping method, that extracts city and OS type for all records with bid > 250
	 * from logs
	 *
	 * @param key
	 *            byte key
	 * @param value
	 *            input string
	 * @param context
	 *            Mapper context
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] logsSplitter = value.toString().split("\t");
		try {
			int bidValue = Integer.parseInt(logsSplitter[BIDDING_PRICE]);
			// checking the bid value.
			if (bidValue > THRESOLD_BID_VALUE) {
				int cityId = Integer.parseInt(logsSplitter[CITY_ID]);
				String userAgentString = logsSplitter[USER_AGENT];
				String cityName = cities.get(cityId);

				// when city name is not available from the cache file.
				if (cityName == null) {
					cityName = "unknown";
				}
				// user agent to get the osType.
				UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
				String osType = userAgent.getOperatingSystem().getGroup().getName();
				bidContent.setOsType(osType);
				bidContent.setCityName(cityName);
				bidContent.setCityId(cityId);
				context.write(bidContent, count);
			}
		} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
			// incrementing the counter when exception occur.
			context.getCounter(HighBidCount.MyCounters.ErrorCounters).increment(1);
			logger.info("Unable to parse the line!!!" + e.getMessage());
		}
	}
}
