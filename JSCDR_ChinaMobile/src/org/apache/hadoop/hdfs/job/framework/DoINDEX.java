package org.apache.hadoop.hdfs.job.framework;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.job.FileUtils;
import org.apache.hadoop.hdfs.job.GCTimer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.net.NetUtils;
import org.cProc.GernralRead.ReadData.ParseIndexFormat;
import org.cProc.GernralRead.TableAndIndexTool.TableAndIndexTool;
import org.cProc.query.readFile.ReadBPlusIndexThread;
import org.cProc.sql.SupplementNewFiled;
import org.cProc.tool.Bytes;
import org.cstor.cproc.cloudComputingFramework.CProcFramework;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;
import org.quartz.SchedulerException;

public class DoINDEX implements JobProtocol {

	static {
		Properties props = new Properties();
		try {
			props.load(FileUtils.class.getClassLoader().getResourceAsStream(
					"CFS.properties"));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		String pre = props.getProperty("pre");
		if (pre == null)
			pre = "";
		TableAndIndexTool.setPrePath(pre);
		System.out.println("pre---------------------------------------------->"
				+ pre);
		try {
			new GCTimer();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static final Log LOG = LogFactory.getLog(DoINDEX.class.getName());
	public Configuration confg = null;
	public ClientProtocol nameNode = null;
	public ClientDatanodeProtocol dataNode = null;
	public Job job = null;
	private static String confPath = null;
	HashMap<String, StringBuffer> result = new HashMap<String, StringBuffer>();
	private List<byte[]> reslutData = new ArrayList<byte[]>();

	public DoINDEX() {
		Configuration conf = new Configuration();
		String confUrl = conf.get("dfs.hadoop.conf");
		this.confPath = confUrl + "/hdfs-site.xml";
		// LOG.info("BS_DoINDEX =======================================================");
	}

	public DoINDEX(DataNode dataNode, Job e) {
		this.job = e;
		this.dataNode = dataNode;
		Configuration conf = new Configuration();
		String confUrl = conf.get("dfs.hadoop.conf");
		this.confPath = confUrl + "/hdfs-site.xml";
		// LOG.info("BS_DoINDEX (DataNode dataNode, Job e) @xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+confUrl);
	}

	@Override
	public HashMap<String, StringBuffer> handle() {
		//新增状态
		
		String NNip =	job.getConf().get("hdfs.job.NN.ip");
        NNip =	NNip.split(":")[0] + ":" + job.getConf().getInt("cproc.node.port", Const.CONNECTION_PORT) ;
//        LOG.info("NNip  : " +NNip);
        CProcFrameworkProtocol frameworkNode1 = null;
        InetSocketAddress frameworkNodeAddr1 = NetUtils.createSocketAddr(NNip);			
        LOG.info(" job.getConf().get(\"hdfs.job.jobstutas\")  : " +job.getConf().get("hdfs.job.jobstutas"));	
        if(!job.getConf().get("hdfs.job.jobstutas","true").equals("false")){
	    try {
		      frameworkNode1 = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
				   CProcFrameworkProtocol.versionID, frameworkNodeAddr1, confg,
		           NetUtils.getSocketFactory(confg, CProcFrameworkProtocol.class));
		       frameworkNode1.changeJobEntityType(job.getConf().get("hdfs.job.jobid"), JobEntityType.INDEX);
	        } catch (IOException e) {
		        e.printStackTrace();
	        }
//	        finally{
//	        	RPC.stopProxy(frameworkNode1);
//	        }
        }
		LOG.info("HashMap<String, StringBuffer> handle() ++++++++++++++++++++++++++++++++++++++++"
				+ confPath);
		HashMap<String, StringBuffer> DNIPtoFileandOffsets = new HashMap<String, StringBuffer>();
		LOG.info("******************************china_mobile**************************************");
		LOG.info("*                                                                                                                                          *");
		LOG.info("*                         BS_DoINDEX is running!!!                                                                          *");
		LOG.info("*                                                                                                                                          *");
		LOG.info("*********************************************************************************");
		LOG.info("MyJob:" + "cProc.node.port : " + job.getConf().get("cproc.node.port"));
		LOG.info("this job come from " + job.getConf().get("hdfs.job.from.ip"));
		// LOG.info("hdfs.job.param.inputpara : "+
		// job.getConf().get("hdfs.job.param.inputpara"));
		this.job.getConf().set("hdfs.job.class",
				"org.apache.hadoop.hdfs.job.framework.DoSEARCH");
		job.getConf().set("hdfs.job.from.ip", getHostIP.getLocalIP());
		String[] index2Files = job.getConf().getStrings(
				"hdfs.job.param.system.param");
		
		if (index2Files == null) {
			LOG.info("index2Files == null");
			return null;
		}

		Arrays.sort(index2Files);

		// LOG.info("hdfs.job.param.system.param : "+
		// job.getConf().get("hdfs.job.param.system.param"));
		String sessionId = job.getConf().get("hdfs.job.param.sessionId");

		LocatedBlocks[] lbArr = null;
		//
		long start = System.currentTimeMillis();

		try {
			// nameNode = (ClientProtocol)
			// RPC.getProxy(ClientProtocol.class,ClientProtocol.versionID,
			// nameNodeAddr, confg, NetUtils.getSocketFactory(confg,
			// ClientProtocol.class));
			lbArr = nameNode.getBlockLocationsForMutilFile(index2Files, 0, 777);
		} catch (IOException e) {
			e.printStackTrace();
		}

		LOG.info("  getBlockLocationsForMutilFile cw cost--->"
				+ (System.currentTimeMillis() - start));
		start = System.currentTimeMillis();
		Block[] locatedBlocks = new Block[lbArr.length];
		int i = 0;
		for (LocatedBlocks lb : lbArr) {
			locatedBlocks[i++] = lb.get(0).getBlock();
		}

		String[] locatedFiles = null;
		LOG.info("******************************xxx****************************************");
		String path = job.getConf().get("cProc.path");
		String sql = job.getConf().get("cProc.cdrSQL");
		String hdfsPath = job.getConf().get("cProc.hdfs");

		try {
			// LOG.info("this.dataNode-------->"+this.dataNode);
			locatedFiles = this.dataNode.getBlockFiles(locatedBlocks);
			// LOG.info("------------------------------------3");

			LOG.info("  getBlockFiles cw cost--->"
					+ (System.currentTimeMillis() - start));
			// //debug
//			 {
//			 for(int xxx=0;xxx<index2Files.length;xxx++)
//			 {
//				System.out.println("cw:logic path------>"+index2Files[xxx]+"      disk path---->"+locatedFiles[xxx]);
//			 }
//			
//			 }
			// end

			Integer condition = new Integer(0);
			ArrayList<ReadBPlusIndexThread> threadList = ReadBPlusIndexThread
					.InitReadThread(confPath, condition);

			// sqlFormat: SQL-_-!indexPath
			ParseIndexFormat readIndexFormat = null;

			// System.out.println("sql is " + sql);

			FileSystem fs = null;
			try {
				fs = FileSystem.get(this.confg);
				if (fs != null) {
					readIndexFormat = new ParseIndexFormat(path, fs);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			// start_time_s == 1321340888 and
			String fullSQL = SupplementNewFiled.getSQL(sql, readIndexFormat);
			// System.out.println("fullSQL is " + fullSQL);

			sql = fullSQL + "-_-!" + path;
			List<String> inValidPath = new ArrayList<String>();

			int locatedFile_length = locatedFiles.length - 1;

			for (int oi = locatedFile_length; oi >= 0; oi--) {
				if (locatedFiles[oi].equals("false")) {

					LOG.warn("bad block ,hdfs name-->" + index2Files[oi]);
					inValidPath.add(index2Files[oi]);
					continue;
				}
				for (int _i = 0; _i < threadList.size(); _i++) {
					threadList.get(_i).addReadIndex(locatedFiles[oi], sql,
							hdfsPath);
				}
			}

			// boolean first = false;

			LOG.info("------------------DoINDEX1----------------222-------");
			start = System.currentTimeMillis();
			for (int _i = 0; _i < threadList.size(); _i++) {
				Thread thread = new Thread(threadList.get(_i));
				thread.start();
			}
			// LOG.info("------------------DoINDEX1----------------333-------");
			while (!ReadBPlusIndexThread.ThreadsIsOver(threadList)) {
				synchronized (condition) {
					try {
						if (!ReadBPlusIndexThread.ThreadsIsOver(threadList)) {
							condition.wait(6000);
							for (int _i = 0; _i < threadList.size(); _i++) {
								threadList.get(_i).getResultData(reslutData);
							}
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

			for (int _i = 0; _i < threadList.size(); _i++) {
				threadList.get(_i).getResultData(reslutData);
			}
			LOG.info("  ReadBPlusIndexThread cw cost--->"
					+ (System.currentTimeMillis() - start));

			// 对于无效的block
			if (!inValidPath.isEmpty()) {
				LOG.info("some block bad :---->" + inValidPath);
				List<File> inValidToPath = new ArrayList<File>(
						inValidPath.size());
				// 下载到本地,必须下载到hdfs的一个挂载盘
				String disks = new Configuration().get("dfs.data.dir");
				String[] offsets = disks.split(",");
				String pre = "/";
				if (offsets != null && offsets.length > 0) {
					pre = offsets[0].trim();
				}
				disks = null;
				offsets = null;

				for (String tempPath : inValidPath) {
					File file = new File(pre + java.io.File.separator
							+ java.util.UUID.randomUUID().toString());
					Tools.downloadFromHdfs(tempPath, file.getAbsolutePath(), fs);
					inValidToPath.add(file);
				}

				//
				ArrayList<ReadBPlusIndexThread> inValidthreadList = ReadBPlusIndexThread
						.InitReadThread(confPath, condition);
				for (int oi = 0; oi < inValidToPath.size(); oi++) {
					for (int _i = 0; _i < inValidthreadList.size(); _i++) {
						inValidthreadList.get(_i).addReadIndex(
								inValidToPath.get(oi).getAbsolutePath(), sql,
								hdfsPath);
					}
				}

				for (int _i = 0; _i < inValidthreadList.size(); _i++) {
					Thread thread = new Thread(inValidthreadList.get(_i));
					thread.start();
				}
				// LOG.info("------------------DoINDEX1----------------333-------");
				while (!ReadBPlusIndexThread.ThreadsIsOver(inValidthreadList)) {
					synchronized (condition) {
						try {
							if (!ReadBPlusIndexThread
									.ThreadsIsOver(inValidthreadList)) {
								condition.wait(6000);
								for (int _i = 0; _i < inValidthreadList.size(); _i++) {
									inValidthreadList.get(_i).getResultData(
											reslutData);
								}
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				for (int _i = 0; _i < inValidthreadList.size(); _i++) {
					inValidthreadList.get(_i).getResultData(reslutData);
				}
				//
				for (File file : inValidToPath) {
					file.delete();
				}

				inValidToPath.clear();
				inValidToPath = null;
				// TODO
			}
			inValidPath.clear();
			inValidPath = null;
			job.getConf().set("hdfs.job.jobstutas", "ture");
			LOG.info("------------------END-------------------------");
			LOG.info("reslutData.size() : " + reslutData.size());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// List<String> tempResultList = new ArrayList<String>();
		// List<byte[]> tempResul = new ArrayList<byte[]>();
		// for (int zk = 0; zk < reslutData.size(); zk++) {
		//
		// String temp = Bytes.toString(reslutData.get(zk));
		//
		// String zzzzzz = temp.split("#")[0];
		//
		//
		// if (tempResultList.contains(zzzzzz))
		// {
		// LOG.info("already exists:" +zzzzzz);
		// }else
		// {
		// tempResultList.add(zzzzzz);
		// // tempResul.add(reslutData.get(zk));
		// }
		// }

		String[] dataFiles = new String[reslutData.size()];
		String[] dataOffset = new String[reslutData.size()];
		i = 0;
		String temp = null;
		String[] temp1 = null;
		// int zzzz = 0;
		LOG.info("tempResul.size() : " + reslutData.size());
		for (int j = 0; j < reslutData.size(); j++) {
			temp = Bytes.toString(reslutData.get(j));

			temp1 = temp.split("#");

			dataFiles[j] = temp1[0];

			dataOffset[j] = temp1[1];

			// zzzz += temp1[1].split(",").length;
		}

		String DNIP = "";
		start = System.currentTimeMillis();
		try {
			lbArr = nameNode.getBlockLocationsForMutilFile(dataFiles, 0, 777);
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("22cw cost--->" + (System.currentTimeMillis() - start) / 1000);
		i = 0;
		String timesStr = job.getConf().get(Const.DATANODE_TIMEOUT_TIMES);
		int timeInt = Const.DEFAULT_DATANODE_TIMEOUT_TIMES_VALUE;
		if (timesStr != null) {
			try {
				timeInt = Integer.parseInt(timesStr.trim());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for (int j = 0; j < dataOffset.length; j++) {

			LocatedBlocks blocks = lbArr[i++];
			if (blocks == null) {
				
				int __temp = i;
				LOG.warn("source file  skip:" + dataFiles[--__temp]);
				continue;
			}

			if (blocks.getLocatedBlocks() == null
					|| blocks.getLocatedBlocks().isEmpty()) {
				int __temp = i;
				LOG.warn("source file  skip:" + dataFiles[--__temp]);
				continue;
			}

			DatanodeInfo[] dis = blocks.getLocatedBlocks().get(0)
					.getLocations();

			DatanodeDescriptor[] timeOutDatanode = nameNode
					.getInvalidDNs(timeInt);
			dis = Tools.KickTimeOutDatanode(dis, timeOutDatanode);

			// for(DatanodeInfo d : dis){
			// sb.append(d.getHost()+",");
			// }
			// LOG.info("MyJob:" +"dis:"+sb);
			if (dis == null || dis.length == 0) {
				// 跳过
				int __temp = i;

				LOG.warn("source file  skip:" + dataFiles[--__temp]);
				continue;
			}

			DatanodeInfo di = dis[((int) (Math.random() * 1000)) % dis.length];

			DNIP = di.getHost() + ":" + di.getIpcPort();

			String offsets = dataOffset[j];

			if (!DNIPtoFileandOffsets.containsKey(DNIP)) {

				DNIPtoFileandOffsets.put(DNIP, (new StringBuffer(sessionId
						+ "!@" + dataFiles[j] + " ")).append(offsets));
			} else {
				DNIPtoFileandOffsets.put(DNIP, DNIPtoFileandOffsets.get(DNIP)
						.append("#" + dataFiles[j] + " ").append(offsets));
			}
		}
		// for (String dataFile : result.keySet()) {
		//
		// DatanodeInfo[] dis = lbArr[i++].getLocatedBlocks().get(0)
		// .getLocations();
		//
		// DatanodeInfo di = dis[((int) (Math.random() * 1000)) % dis.length];
		//
		// DNIP = di.getHost() + ":" + di.getIpcPort();
		//
		// StringBuffer offsets = result.get(dataFile);
		//
		// if (!DNIPtoFileandOffsets.containsKey(DNIP)) {
		// DNIPtoFileandOffsets.put(DNIP, (new StringBuffer(sessionId
		// + "!@" + dataFile + " ")).append(offsets));
		// } else {
		// DNIPtoFileandOffsets.put(DNIP, DNIPtoFileandOffsets.get(DNIP)
		// .append("#" + dataFile + " ").append(offsets));
		// }
		// // LOG.info(paths[i]+"--"+blocks[i].getBlockName());
		// }
		// 空任务
		// String[] DNIPs = job.getConf().getStrings("hdfs.job.DNIPs");
		// if (DNIPtoFileandOffsets.keySet().size() < DNIPs.length) {
		// for (String DNIp : DNIPs) {
		// if (!DNIPtoFileandOffsets.keySet().contains(DNIp)) {
		// LOG.info("there is no DNIp : " + DNIp);
		// DNIPtoFileandOffsets.put(DNIp, new StringBuffer(sessionId
		// + "!@"));
		// }
		// LOG.info("DNIp : " + DNIp);
		// // LOG.info("DNIPtoFileandOffsets.get(DNIp) : " +
		// // DNIPtoFileandOffsets.get(DNIp));
		// }
		// }
		// 发送success
		if (DNIPtoFileandOffsets.isEmpty() || DNIPtoFileandOffsets.size() == 0) {
//			String NNip = job.getConf().get(Const.NNIP);
//			NNip = NNip.split(":")[0] + ":"
//					+ confg.getInt("cproc.node.port", Const.CONNECTION_PORT);
//			LOG.info("NNip  : " + NNip);
			CProcFrameworkProtocol frameworkNode = null;
			InetSocketAddress frameworkNodeAddr = NetUtils
					.createSocketAddr(NNip);
			LOG.info(" job.getConf().get(\"hdfs.job.jobstutas\")  : "
					+ job.getConf().get("hdfs.job.jobstutas"));
			if (!job.getConf().get("hdfs.job.jobstutas", "true")
					.equals("false")) {
				try {
					frameworkNode = (CProcFrameworkProtocol) RPC.getProxy(
							CProcFrameworkProtocol.class,
							CProcFrameworkProtocol.versionID,
							frameworkNodeAddr, confg, NetUtils
									.getSocketFactory(confg,
											CProcFrameworkProtocol.class));
					frameworkNode.changeJobEntityType(
							job.getConf().get("hdfs.job.jobid"),
							JobEntityType.SUCCESS);
				} catch (IOException e) {
					e.printStackTrace();
				}
//				finally{
//					RPC.stopProxy(frameworkNode);
//				}
			}
		}
		// for (String DNIp : DNIPtoFileandOffsets.keySet()) {
		// LOG.info("DNIp : " + DNIp);
		// }

		LOG.info("++++++++++++++++++++++++++++++ DoINDEX1 ++++++++++++++++++++++++++++++"
				+ System.currentTimeMillis());
		// try {
		// DNCountUtil dnCountUtil = new DNCountUtil();
		// int count2 =dnCountUtil.checkToMem(job, "offset_begin_search_test",
		// 10000);
		// LOG.info("==================All count :================="+count2);
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// Tools.printMap(DNIPtoFileandOffsets);
		
		
		return DNIPtoFileandOffsets;
		// return null;
	}

	private void sendReslutData(List<byte[]> reslutData, String sessionId,
			String NNip) {

		LOG.info("---------------- DoINDEX.sendReslutData ---------------------------");

		HashMap<String, StringBuffer> DNIPtoFileandOffsets = new HashMap<String, StringBuffer>();
		LocatedBlocks[] lbArr = null;
		List<String> tempResultList = new ArrayList<String>();
		List<byte[]> tempResul = new ArrayList<byte[]>();

		// --------------------------------------Handle the reslutData
		// set-------------------------------------
		for (int zk = 0; zk < reslutData.size(); zk++) {

			String temp = new String(reslutData.get(zk));
			String zzzzzz = temp.split("#")[0];

			if (tempResultList.contains(zzzzzz)) {
				LOG.info("already exists:" + zzzzzz);
			} else {
				tempResultList.add(zzzzzz);
				// tempResul.add(reslutData.get(zk));
			}
		}

		String[] dataFiles = new String[reslutData.size()];
		String[] dataOffset = new String[reslutData.size()];
		int i = 0;
		String temp = null;
		String[] temp1 = null;
		int zzzz = 0;
		LOG.info("tempResul.size() : " + reslutData.size());
		for (int j = 0; j < reslutData.size(); j++) {
			temp = new String(reslutData.remove(0));
			temp1 = temp.split("#");

			dataFiles[j] = temp1[0];

			dataOffset[j] = temp1[1];

			zzzz += temp1[1].split(",").length;
		}

		String DNIP = "";
		try {
			lbArr = nameNode.getBlockLocationsForMutilFile(dataFiles, 0, 777);
		} catch (IOException e) {
			e.printStackTrace();
		}
		i = 0;
		for (int j = 0; j < dataOffset.length; j++) {

			DatanodeInfo[] dis = lbArr[i++].getLocatedBlocks().get(0)
					.getLocations();

			DatanodeInfo di = null;

			for (DatanodeInfo tempDI : dis) {

				if (tempDI.getHost().equals(
						getHostIP.getLocalIP(CProcFramework.network))) {
					di = tempDI;
					break;
				}
			}

			if (di == null)
				di = dis[((int) (Math.random() * 1000)) % dis.length];

			DNIP = di.getHost() + ":" + di.getIpcPort();

			String offsets = dataOffset[j];

			if (!DNIPtoFileandOffsets.containsKey(DNIP)) {
				DNIPtoFileandOffsets.put(DNIP, (new StringBuffer(sessionId
						+ "!@" + dataFiles[j] + " ")).append(offsets));
			} else {
				DNIPtoFileandOffsets.put(DNIP, DNIPtoFileandOffsets.get(DNIP)
						.append("#" + dataFiles[j] + " ").append(offsets));
			}
		}

		String[] DNIPs = job.getConf().getStrings("hdfs.job.DNIPs");
		if (DNIPtoFileandOffsets.keySet().size() < DNIPs.length) {
			for (String DNIp : DNIPs) {
				if (!DNIPtoFileandOffsets.keySet().contains(DNIp)) {
					LOG.info("there is no DNIp : " + DNIp);
					DNIPtoFileandOffsets.put(DNIp, new StringBuffer(sessionId
							+ "!@"));
				}
				LOG.info("DNIp : " + DNIp);
				// LOG.info("DNIPtoFileandOffsets.get(DNIp) : " +
				// DNIPtoFileandOffsets.get(DNIp));
			}
		}
		// ---------------------------------------distribute data to
		// DN------------------------------------
		// for (String DNIp : DNIPtoFileandOffsets.keySet()) {
		// LOG.info("DNIp : " + DNIp);
		// }

		Configuration confg = new Configuration();

		Job newJob = new Job();

		newJob.setConf(this.job.getConf());

		newJob.setJobName(this.job.getJobName());

		newJob.getConf().set("hdfs.job.jobstutas", "false");

		// job.setJobName(new Text("myjob"));

		LOG.info("NNip  : " + NNip);

		NNip = NNip.split(":")[0] + ":"
				+ job.getConf().getInt("cproc.node.port", Const.CONNECTION_PORT);

		LOG.info("NNip  : " + NNip);

		LOG.info("NNip  : " + newJob.getConf().get("hdfs.job.jobstutas"));

		CProcFrameworkProtocol frameworkNode = null;
		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(NNip);

		// LOG.info(" frameworkNodeAddr.getHostString()  : " +
		// frameworkNodeAddr.getHostString());
		LOG.info(" frameworkNodeAddr.getPort()  : "
				+ frameworkNodeAddr.getPort());

		try {
			frameworkNode = (CProcFrameworkProtocol) RPC.getProxy(
					CProcFrameworkProtocol.class,
					CProcFrameworkProtocol.versionID, frameworkNodeAddr, confg,
					NetUtils.getSocketFactory(confg,
							CProcFrameworkProtocol.class));
			// frameworkNode.submitJob(job);

		} catch (IOException e) {
			e.printStackTrace();
		}

		CProcFrameworkProtocol CProcFrameworkNode = null;
		InetSocketAddress CProcFrameworkNodeAddr = null;

		Set<String> DNs = DNIPtoFileandOffsets.keySet();

		String CProcFrameworkNodeIP = null;

		if (DNs.size() == 0)
			return;

		// frameworkNode.changeJobEntityType(newJob.getJobId().toString(),
		// JobEntityType.RUNNING);
		//
		// frameworkNode.changeJobEntityType(newJob.getConf().get("hdfs.job.jobid")
		// , JobEntityType.RUNNING);
		for (String DN : DNs) {
			CProcFrameworkNodeIP = DN.split(":")[0] + ":"
					+ job.getConf().get("cproc.node.port", Const.CONNECTION_PORT + "");

			CProcFrameworkNodeAddr = NetUtils
					.createSocketAddr(CProcFrameworkNodeIP);

			try {
				CProcFrameworkNode = (CProcFrameworkProtocol) RPC.getProxy(
						CProcFrameworkProtocol.class,
						CProcFrameworkProtocol.versionID,
						CProcFrameworkNodeAddr, confg, NetUtils
								.getSocketFactory(confg,
										CProcFrameworkProtocol.class));
				LOG.info("CProcFrameworkNodeAddr : " + CProcFrameworkNodeAddr);

				newJob.getConf().set("hdfs.job.param.system.param",
						DNIPtoFileandOffsets.get(DN).toString());

				// LOG.info("j.getConf().get(\"hdfs.job.jobid\") + \",\" + DN" +
				// j.getConf().get("hdfs.job.jobid") + "," + DN);

				// frameworkNode.changeJobEntityType(newJob.getConf().get("hdfs.job.jobid")
				// + "," + DN.split(":")[0], JobEntityType.RUNNING);
				//
				// frameworkNode.printJobEntityType(newJob.getJobId().toString());

				CProcFrameworkNode.submitJob(newJob);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOG.info("+++++++++++++++++++ DoINDEX.sendReslutData ++++++++++++++++");
	}

	@Override
	public Configuration getConfiguration() {
		return this.confg;
	}

	public ClientDatanodeProtocol getDataNode() {
		return this.dataNode;
	}

	@Override
	public Job getJob() {
		return this.job;
	}

	public ClientProtocol getNameNode() {
		return this.nameNode;
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.confg = conf;
	}

	@Override
	public void setDataNode(ClientDatanodeProtocol datanode) {
		this.dataNode = datanode;

	}

	@Override
	public void setJob(Job j) {
		this.job = j;
	}

	@Override
	public void setNameNode(ClientProtocol namenode) {
		this.nameNode = namenode;
	}

	public void stop() {
		LOG.info("-----------DoINDEX1 stop!!------------");
	}

	public long readLong(char[] temp) {
		long tempLong = 8;
		if (temp != null && temp.length > 0) {
			tempLong = Long.parseLong(String.valueOf(temp));
		}
		return tempLong;
	}

	@Override
	public void setCProcFrameworkProtocol(CProcFrameworkProtocol arg0) {
		// TODO Auto-generated method stub

	}

	// public queryConditions paraToObject(String para) {
	//
	// CDRBeanForIndex cdr = new CDRBeanForIndex(JSONObject.fromObject(para));
	//
	// long[] startTime = cdr.getStart_time_s();
	//
	// long[] endTime = cdr.getEnd_time_s();
	//
	// long[] opc = cdr.getOpc();
	//
	// long[] dpc = cdr.getDpc();
	//
	// int tableType = cdr.getTableType();
	//
	// String cdrType = cdr.getCdr_type();
	//
	// int callType = cdr.getCDRCallType();
	//
	// String phoneNum = String.valueOf(cdr.getCalled_number());
	//
	// if (phoneNum == null || "".equals(phoneNum) || "null".equals(phoneNum))
	//
	// phoneNum = String.valueOf(cdr.getCalling_number());
	//
	// int wangYuanType = cdr.getNetElem();
	//
	// String wangYuan = cdr.getNetElemId();
	//
	// queryConditions query;
	//
	// if (tableType != 2) {
	// query = new queryConditions(startTime, endTime, tableType, cdrType,
	// callType, phoneNum, wangYuanType, wangYuan);
	// } else {
	// query = new queryConditions(startTime, endTime, tableType,
	// callType, opc, dpc, phoneNum);
	// }
	//
	// return query;
	// }

}
