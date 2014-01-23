package  org.apache.hadoop.hdfs.job.framework;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.job.TableDecode;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.net.NetUtils;
import org.cProc.index.QueryTargetIndex;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;


public class DoNNDIR implements JobProtocol{
	public static final Log LOG = LogFactory.getLog(DoNNDIR.class.getName());
	public  Configuration confg = null; 
	public  ClientProtocol nameNode = null;
	public  ClientDatanodeProtocol dataNode = null;
	public  Job job = null;
	
	public FileSystem fs = null;
	public DoNNDIR() { 
    }
	
	public DoNNDIR(NameNode nameNode,Job e,Configuration confg) {
    this.job = e;
    this.nameNode = nameNode;  
    this.confg = confg;    
  }
	
	public HashMap<String,StringBuffer> handle(){
		
		LOG.info("****************************** 002 china_mobile*********************************");
    	LOG.info("*                                                                              *");
    	LOG.info("*                         DoNNDIR is running!!!                                *");
    	LOG.info("*                                                                              *");
    	LOG.info("********************************************************************************");
    	LOG.info("MyJob:" + "cProc.node.port : " + job.getConf().get("cproc.node.port"));
		LOG.info("MyJob:" + "this job come from " + job.getConf().get("hdfs.job.from.ip"));
		LOG.info("MyJob:" + "hdfs.job.param.sessionId : " + job.getConf().get("hdfs.job.param.sessionId"));
		LOG.info("MyJob:" + "hdfs.job.param.indexDir : " + job.getConf().get("hdfs.job.param.indexDir"));
		this.job.getConf().set("hdfs.job.class","org.apache.hadoop.hdfs.job.framework.DoINDEX");
		try {
			fs = FileSystem.get(confg);
		} catch (IOException e3) {
			e3.printStackTrace();
		}
		job.getConf().set("hdfs.job.from.ip",getHostIP.getLocalIP());
		String indexDir = job.getConf().get("hdfs.job.param.indexDir");
		String starttime = job.getConf().get("cProc.starttime");
		String endtime = job.getConf().get("cProc.endtime");
		long s1 = 0;
		long e1 = 0;
		if(starttime!=null && !starttime.equals("") && !starttime.equals("null")){
			s1 = Long.parseLong(starttime);
		}
		if(endtime!=null && !endtime.equals("") && !endtime.equals("null")){
			e1 = Long.parseLong(endtime);
		}
		//job.getConf().set("fs.default.name", this.confg.get("fs.default.name"));
		LOG.info("start time-->"+s1+"    end time-->"+e1);
		if(this.nameNode == null)
			return null;
		//ArrayList<String> indexFilesPathList = new ArrayList<String>();
		HashMap<String,StringBuffer> DNIPtoIndexFiles = new HashMap<String,StringBuffer>();
		String name = job.getConf().get("cProc.table.name");
		LOG.info("cProc.table.name is:--------------------->"+name);
		int type = TableDecode.getTableType(name);
		job.getConf().set("cProc.table.type",type+"");	
		LOG.info("cProc.table.type  is:--------------------->"+type);
		QueryTargetIndex query = new QueryTargetIndex(s1,e1,indexDir,fs);
		
		//LOG.info("s1,e1: " +s1+"  "+e1);
		int i = 0;
		String[] indexFilesPath = null;
		try {
			indexFilesPath = query.getResult();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		query = null;
		if(indexFilesPath==null || indexFilesPath.length==0)	{
			if(!job.getConf().get("hdfs.job.jobstutas","true").equals("false")){
			 try {
					String NNip =	job.getConf().get(Const.NNIP);
					NNip =	NNip.split(":")[0] + ":" +job.getConf().getInt("cproc.node.port",Const.CONNECTION_PORT) ;
					InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(NNip);		
					CProcFrameworkProtocol frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,CProcFrameworkProtocol.versionID, frameworkNodeAddr, confg, NetUtils.getSocketFactory(confg, CProcFrameworkProtocol.class));
					frameworkNode.changeJobEntityType(job.getConf().get("hdfs.job.jobid"), JobEntityType.SUCCESS);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("job start success");
			return null;
		}//
		LOG.info("MyJob: indexFilesPathList.size() : " + indexFilesPath.length);
		LocatedBlocks[] lbArr = null;
		String DNIP = "";
		try {
//			for(int r = 0 ; r<indexFilesPath.length;r++){
//				System.out.println(indexFilesPath[r]);
//			}
			lbArr = nameNode.getBlockLocationsForMutilFile(indexFilesPath,0, 777);
			if(lbArr==null){
				LOG.info("lbArr==null");
			}else{
				LOG.info("lbArr!=null" +lbArr.length );
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		LOG.info("lbArr--->"+lbArr==null);
//		LOG.info("lbArr[0--->"+lbArr[0]==null);
//		int k = 0;
//		for(k = 0;k<lbArr.length;){
//			if(lbArr[k]==null){
//				k++;
//			}
//		}
//		LOG.info("lbArr changdu : "+ k);
		if(lbArr[0]==null){
			LOG.info("lbArr[0]--->null");
		}else{
			LOG.info("lbArr[0]--->!=null");
		}
		if(lbArr[0].getLocatedBlocks()==null){
			LOG.info("lbArr[0].getLocatedBlocks==null");
		}else{
			LOG.info("lbArr[0].getLocatedBlocks()!=null"+lbArr[0].getLocatedBlocks().size());
		}
		
		i = 0;
		//StringBuffer sb = new StringBuffer();
//		LOG.info("MyJob: indexFilesPath.length : " + indexFilesPath.length);
		for(String indexFilePath : indexFilesPath){
//			LOG.info("MyJob: indexFilePath : " + indexFilePath);
			
			DatanodeInfo[] dis = null;
			
			if(lbArr[i].getLocatedBlocks().size() <= 0 	){	
				LOG.warn("indexFilePath skip   getLocatedBlocks().size："+indexFilePath);
				i++;
				continue;
			}
			
			if(lbArr[i].getLocatedBlocks().get(0).getLocations().length <= 0){
				LOG.warn("indexFilePath skip  getLocations().length  :"+indexFilePath);
				i++;
				continue;
			}
			dis = lbArr[i].getLocatedBlocks().get(0).getLocations();
			//获取超时的次数
			String timesStr = job.getConf().get(Const.DATANODE_TIMEOUT_TIMES);

			int timeInt = Const.DEFAULT_DATANODE_TIMEOUT_TIMES_VALUE;
			if(timesStr!=null)
			{
				try{
					timeInt = Integer.parseInt(timesStr.trim());
				}catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			DatanodeDescriptor[] timeOutDatanode = nameNode.getInvalidDNs(timeInt);
			dis = Tools.KickTimeOutDatanode(dis, timeOutDatanode);
			
			//for(DatanodeInfo d : dis){
				//sb.append(d.getHost()+",");
			//}
			//LOG.info("MyJob:" +"dis:"+sb); 
			if(dis==null || dis.length==0)
			{
				//跳过
				LOG.warn("indexFilePath skip:"+indexFilePath);
				i++;
				continue;
			}
			DatanodeInfo di = dis[((int)(Math.random()*1000))%dis.length];
			
			DNIP = di.getHost() + ":" + di.getIpcPort(); 
			
			//LOG.info("MyJob:" + "indexFilePath : " + indexFilePath + " <----> DNIP : " + DNIP);
			if(!DNIPtoIndexFiles.containsKey(DNIP))
			{
				DNIPtoIndexFiles.put(DNIP,(new StringBuffer(indexFilePath)));
			}
			else
			{
				DNIPtoIndexFiles.put(DNIP,
						DNIPtoIndexFiles.get(DNIP).append(",").append(indexFilePath));
			}	
			
			i++;
			// LOG.info(paths[i]+"--"+blocks[i].getBlockName());
		}	
		
		//如果所有的文件都无效，skip掉
		if(DNIPtoIndexFiles==null ||DNIPtoIndexFiles.isEmpty())
		{
			if(!job.getConf().get("hdfs.job.jobstutas","true").equals("false")){
				 try {
						String NNip =	job.getConf().get(Const.NNIP);
						NNip =	NNip.split(":")[0] + ":" +job.getConf().getInt("cproc.node.port",Const.CONNECTION_PORT) ;
						InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(NNip);		
						CProcFrameworkProtocol frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,CProcFrameworkProtocol.versionID, frameworkNodeAddr, confg, NetUtils.getSocketFactory(confg, CProcFrameworkProtocol.class));
						frameworkNode.changeJobEntityType(job.getConf().get("hdfs.job.jobid"), JobEntityType.SUCCESS);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				System.out.println("job start success");
				return null;
		}
		
		//发送空任务
//		String[] DNIPs = job.getConf().getStrings("hdfs.job.DNIPs");
//	    if(DNIPtoIndexFiles.keySet().size() < DNIPs.length){
//			for(String DNIp : DNIPs){
//				if(!DNIPtoIndexFiles.keySet().contains(DNIp)){
//					LOG.info("there is no DNIp : " + DNIp);			
//					DNIPtoIndexFiles.put(DNIP,new StringBuffer(""));
//				}
//			}
//		}
		
		
		Tools.printMap(DNIPtoIndexFiles);
		
//		for(String DNIp : DNIPtoIndexFiles.keySet()){
//			StringBuffer indexFiles = DNIPtoIndexFiles.get(DNIp);
//			LOG.info("DNIp : " + DNIp);
//			LOG.info("indexFiles============================================= : " + indexFiles);			
//		}
		
		
		LOG.info("++++++++++++++++++++++++++++++DoNNDIR++++++++++++++++++++DNIPtoIndexFiles size+++++++++");
		////////////////////////////////
//		Configuration conf = job.getConf();
//		conf.set("dfs.index.count", DNIPtoIndexFiles.size()+"");
//        DNCountUtil dnCountUtil = new DNCountUtil();
//		int count = dnCountUtil.getDNIps(DNIPtoIndexFiles, job);
//		LOG.info("==================DNIP count :================="+count);
	
		return DNIPtoIndexFiles;
		//return null;
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

	@Override
	public void setCProcFrameworkProtocol(CProcFrameworkProtocol arg0) {
		// TODO Auto-generated method stub
		
	}
}
