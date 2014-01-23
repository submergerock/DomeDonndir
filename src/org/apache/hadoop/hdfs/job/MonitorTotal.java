package org.apache.hadoop.hdfs.job;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MonitorTotal implements Runnable {

	public static final ConcurrentMap<String, AtomicLong> SEND_COUNT = new ConcurrentHashMap<String, AtomicLong>();
	public static final ConcurrentMap<AtomicLong, String> CREATE_TIME = new ConcurrentHashMap<AtomicLong, String>();
	public static final long SLEEP = 1000 * 60 * 60;
	public static final long DIS = 1000 * 60 * 5;
	public static long TOTAL;
	public static final Log LOG = LogFactory.getLog(MonitorTotal.class
			.getName());

	static {
		Properties prop = new Properties();
		try {
			prop.load(MonitorTotal.class
					.getResourceAsStream("total.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (prop.getProperty("total") != null) {
			try {
				TOTAL = Long.parseLong(prop.getProperty("total").trim());
			} catch (Exception e) {
				e.printStackTrace();
				TOTAL = 10000;
			}
		} else {
			TOTAL = 10000;
		}
		LOG.info("total-->" + TOTAL);

	}

	public static boolean needSend(final String sessionId, final int count) {

		if (SEND_COUNT.get(sessionId) == null) {
			SEND_COUNT.put(sessionId, new AtomicLong(count));
			CREATE_TIME.put(new AtomicLong(System.currentTimeMillis()),
					sessionId);
			return true;
		}

		long current = SEND_COUNT.get(sessionId).get();
		if (current > TOTAL) {
			return false;
		}

		SEND_COUNT.get(sessionId).addAndGet(count);
		return true;

	}

	public void run() {

		while (true) {

			System.gc();
			try {
				Thread.sleep(SLEEP);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (CREATE_TIME.isEmpty())
				continue;

			long current = System.currentTimeMillis();
			Set<AtomicLong> set = CREATE_TIME.keySet();
			for (AtomicLong temp : set) {
				if ((current - temp.get()) > DIS) {
					String sessionId = CREATE_TIME.get(temp);
					LOG.info("remove --sessionId---->" + sessionId);
					SEND_COUNT.remove(sessionId);
					CREATE_TIME.remove(temp);
					sessionId = null;
				}
			}
		}
	}

}
