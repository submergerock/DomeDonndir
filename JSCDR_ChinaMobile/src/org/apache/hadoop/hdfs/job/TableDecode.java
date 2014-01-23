package org.apache.hadoop.hdfs.job;

import com.dinglicom.decode.util.CDRFieldUtil;

public class TableDecode {

	public static final int BSSAP = 1;
	public static final int BICC = 2;
	public static final int IUCS = 3;
	public static final int MAP = 4;
	public static final int CAP = 5;

	public static final int ABISCDR = 6;

	public static final int ABISMR = 7;

	public static final int ABISHO = 8;

	public static int getTableType(final String name) {
		// CDRFieldUtil.getCDRField("abiscdr");
		if ("bssap".equalsIgnoreCase(name)) {
			return BSSAP;
		} else if ("bicc".equalsIgnoreCase(name)) {
			return BICC;
		} else if ("iucs".equalsIgnoreCase(name)) {
			return IUCS;
		} else if ("map".equalsIgnoreCase(name)) {
			return MAP;
		} else if ("cap".equalsIgnoreCase(name)) {
			return CAP;
		} else if ("abiscdr".equalsIgnoreCase(name)) {
			return ABISCDR;
		} else if ("abismr".equalsIgnoreCase(name)) {
			return ABISMR;
		} else if ("abisho".equalsIgnoreCase(name)) {
			return ABISHO;
		}
		return -1;
	}

}
