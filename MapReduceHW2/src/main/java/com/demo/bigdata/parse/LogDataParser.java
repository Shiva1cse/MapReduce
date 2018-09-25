/**
 * Too parse the data input get the ip and then bytes size. 
 */
package com.demo.bigdata.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.demo.bigdata.exception.PatternNotMatchedException;

/**
 * @author Shiva_Donkena LogDataParser for holding the ipaddress, bytes sizes.
 */
public class LogDataParser {

	private String ipAddr;
	private long byteSize;
	private static final String LOG_LINE_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
	private static Pattern PATTERN;

	private LogDataParser(String ipAddress, String contentSize) throws NumberFormatException {
		this.ipAddr = ipAddress;
		this.byteSize = Long.parseLong(contentSize);
	}

	/**
	 * @return ipAddr Used to get the ip address.
	 */
	public String getIpAddress() {
		return ipAddr;
	}

	public long getByteSize() {
		return byteSize;
	}

	// to check the pattern of the input data.
	public static LogDataParser praseLogRequestLine(String line)
			throws NumberFormatException, PatternSyntaxException, PatternNotMatchedException {
		PATTERN = Pattern.compile(LOG_LINE_ENTRY_PATTERN);
		Matcher matcher = PATTERN.matcher(line);
		if (!matcher.find()) {
			throw new PatternNotMatchedException("Cannot parse " + line);
		}
		return new LogDataParser(matcher.group(1), matcher.group(9));
	}
}
