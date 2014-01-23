package org.apache.hadoop.hdfs.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dinglicom.decode.bean.AbisCloudExpCdrCDR;
import com.dinglicom.decode.bean.AbisCloudExpHoCdrCDR;
import com.dinglicom.decode.bean.AbisCloudExpMrCDR;
import com.dinglicom.decode.bean.BiccCDR;
import com.dinglicom.decode.bean.BssapCDR;
import com.dinglicom.decode.bean.CapCDR;
import com.dinglicom.decode.bean.IucsCDR;
import com.dinglicom.decode.bean.MapCDR;

public class JavaToJson {

	private static final int version = 1;
	
	
	public static final Log LOG = LogFactory.getLog(JavaToJson.class.getName());
	//
	public static List<String> getCDRValue(final byte[] content,
			final int tableType) throws IOException {
		
		
	
		
		
		if (tableType == TableDecode.BSSAP)// 
		{
			return getBSSAPCDRValue(content, version);

		} else if (tableType == TableDecode.BICC) {
			return getBICCCDRValue(content, version);

		} else if (tableType == TableDecode.IUCS) {
			return getIUCSCDRValue(content, version);
		} else if (tableType == TableDecode.MAP) {
			return getMAPCDRValue(content, version);

		} else if (tableType == TableDecode.CAP) {
			return getCAPCDRValue(content, version);
		}

		else if (tableType == TableDecode.ABISCDR) {
			return getABISCDRValue(content, version);
		}

		else if (tableType == TableDecode.ABISMR) {
			return getABISMRValue(content, version);
		}

		else if (tableType == TableDecode.ABISHO) {
			return getABISHOValue(content, version);
		}

		return null;
	}

	// bssap
	private static List<String> getBSSAPCDRValue(byte[] content, int version)
			throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		BssapCDR bssapcdr = new BssapCDR(version);

		if (bssapcdr.decode(content)) {
			listString.add(bssapcdr.toString());
			//LOG.info(listString.get(0));
		}
		bssapcdr = null;
		return listString;
	}

	// bicc
	private static List<String> getBICCCDRValue(byte[] content, int version)
			throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		BiccCDR bicccdr = new BiccCDR(version);

		if (bicccdr.decode(content)) {
			listString.add(bicccdr.toString());
		}
		bicccdr = null;
		return listString;
	}

	// iucs
	private static List<String> getIUCSCDRValue(final byte[] content,
			int version) throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		IucsCDR iucscdr = new IucsCDR(version);

		if (iucscdr.decode(content)) {
			listString.add(iucscdr.toString());
		}
		iucscdr = null;
		return listString;
	}

	// map
	private static List<String> getMAPCDRValue(final byte[] content, int version)
			throws IOException {
		//
		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		MapCDR mapcdr = new MapCDR(version);

		if (mapcdr.decode(content)) {
			listString.add(mapcdr.toString());
		}
		mapcdr = null;
		return listString;
	}

	// cap
	private static List<String> getCAPCDRValue(final byte[] content, int version)
			throws IOException {
		//
		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		CapCDR capcdr = new CapCDR(version);

		if (capcdr.decode(content)) {
			listString.add(capcdr.toString());
		}
		capcdr = null;
		return listString;
	}
	//abiscdr
	private static List<String> getABISCDRValue(byte[] content, int version)
			throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		AbisCloudExpCdrCDR abiscdr = new AbisCloudExpCdrCDR(version);

		if (abiscdr.decode(content)) {
			listString.add(abiscdr.toString());
		}
		abiscdr = null;
		return listString;
	}
	
	//abismr
	private static List<String> getABISMRValue(byte[] content, int version)
			throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		AbisCloudExpMrCDR abismr = new AbisCloudExpMrCDR(version);

		if (abismr.decode(content)) {
			listString.add(abismr.toString());
		}
		abismr = null;
		return listString;
	}
	
	//abisho
	private static List<String> getABISHOValue(byte[] content, int version)
			throws IOException {

		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		List<String> listString = new ArrayList<String>();
		AbisCloudExpHoCdrCDR abisho = new AbisCloudExpHoCdrCDR(version);

		if (abisho.decode(content)) {
			listString.add(abisho.toString());
		}
		abisho = null;
		return listString;
	}
}
