package org.apache.hadoop.hdfs.job.framework;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;

import com.alibaba.fastjson.JSON;
import com.dinglicom.decode.util.CDRFieldUtil;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		Configuration conf = new Configuration();
//		InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr("192.168.1.111", 9000);
//		ClientProtocol namenode = null;
//		try {
//			namenode = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
//					ClientProtocol.versionID, nameNodeAddr, conf,
//					NetUtils.getSocketFactory(conf, ClientProtocol.class));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		DatanodeDescriptor[] timeOutDatanode = namenode.getInvalidDNs(5);
//		for(DatanodeDescriptor data:timeOutDatanode)
//		{
//			System.out.println(data.getHost());
//		}
		//System.out.println(CDRFieldUtil.getCDRField("MAP"));
		
		System.out.println(File.pathSeparator);

	}

	private static DatanodeInfo[] KickTimeOutDatanode(
			DatanodeInfo[] datanodeinfo, DatanodeDescriptor[] datanodeTimeOut) {
		if (datanodeinfo == null || datanodeinfo.length == 0)
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
}
