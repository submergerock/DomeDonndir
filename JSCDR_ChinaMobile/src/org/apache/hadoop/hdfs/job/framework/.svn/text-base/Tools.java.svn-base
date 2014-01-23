package org.apache.hadoop.hdfs.job.framework;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;

public class Tools {

	public static final Log LOG = LogFactory.getLog(Tools.class.getName());
	public static final int BYTE_SIZE = 1024*1024*100;
	/***
	 * 下载hdfs的文件
	 * @param hdfsPath
	 * @param localPath
	 * @param fs
	 * @throws IOException
	 */
	public static void downloadFromHdfs(String hdfsPath, String localPath,
			FileSystem fs) throws IOException {

		Path hdfs = new Path(hdfsPath);
		FSDataInputStream is = null;

		FileOutputStream fos = null;
		
		try {
			is = fs.open(hdfs);
			fos = new FileOutputStream(localPath);
			int len = -1;
			byte [] bytes = new byte[BYTE_SIZE];
			while(true)
			{
				len = is.read(bytes);
				if(len==-1)
					break;
				fos.write(bytes, 0, len);
			}
			fos.flush();

		} finally {
			if (fos != null) {
				fos.close();
			}
			if (is != null) {
				is.close();
			}

		}

	}

	/**
	 * 删除无效的机器
	 * 
	 * @param datanodeinfo
	 * @param datanodeTimeOut
	 * @return
	 */
	public static DatanodeInfo[] KickTimeOutDatanode(
			final DatanodeInfo[] datanodeinfo,
			final DatanodeDescriptor[] datanodeTimeOut) {
		if (datanodeinfo == null || datanodeinfo.length == 0)
			return datanodeinfo;
		if (datanodeTimeOut == null || datanodeTimeOut.length == 0)
			return datanodeinfo;
		ArrayList<DatanodeInfo> availlist = new ArrayList<DatanodeInfo>();
		boolean flag = true;
		for (int i = 0; i < datanodeinfo.length; i++) {
			String DNIP = datanodeinfo[i].getHostName();
			for (int j = 0; j < datanodeTimeOut.length; j++) {
				String DNIP_timeout = datanodeTimeOut[j].getHostName();
				if (DNIP.equals(DNIP_timeout)) {
					flag = false;
					break;
				}

			}

			if (flag)
				availlist.add(datanodeinfo[i]);
			else
				flag = true;

			DNIP = null;
		}
		return availlist.toArray(new DatanodeInfo[availlist.size()]);
	}
	
	
	public static void printMap(HashMap<String, StringBuffer> handle)
	{
		if(Const.PRINT)
		{
			for(Map.Entry<String, StringBuffer> entry:handle.entrySet())
			{
				LOG.info(entry.getKey()+"----->"+entry.getValue().toString());
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf= new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.1.111:9000/");
		FileSystem fs = FileSystem.get(conf);
		downloadFromHdfs("/smp/cdr/bssap/indexfile/callingIndex/data/2150/2/1300532413_1300536419_3_9128e44c-6fb2-4676-8ba0-d5ff5f65bb19.org","q://111.org",fs);
		

	}

}
