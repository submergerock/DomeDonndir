package org.apache.hadoop.hdfs.job.framework;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.job.FileUtils;
import org.apache.hadoop.hdfs.job.Server;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.cProc.GernralRead.TableAndIndexTool.TableAndIndexTool;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;


public class DoSEARCH implements JobProtocol{
	
	static
	{
		Properties props = new Properties();
		try {
			props.load(FileUtils.class.getClassLoader().getResourceAsStream("CFS.properties"));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		String pre = props.getProperty("pre");
		if(pre==null)
			pre="";
		TableAndIndexTool.setPrePath(pre);
		System.out.println("pre---------------------------------------------->"+pre);
	}
	
	public static final Log LOG = LogFactory.getLog(DoSEARCH.class.getName());
	public  Configuration confg = null; 
	public  ClientProtocol nameNode = null;
	public  ClientDatanodeProtocol dataNode = null;
	public  Job job = null;	
	
	//public  static int count  = 0;	
	
	public DoSEARCH()  {  
		
	  }
	public DoSEARCH(DataNode dataNode,Job e)  {
    this.job = e;
    this.dataNode = dataNode;    
  }
	
	public HashMap<String,StringBuffer> handle(){
		//新增状态
		String NNip =	job.getConf().get("hdfs.job.NN.ip");
        NNip =	NNip.split(":")[0] + ":" +job.getConf().getInt("cproc.node.port",Const.CONNECTION_PORT) ;
//        LOG.info("NNip  : " +NNip);
        CProcFrameworkProtocol frameworkNode1 = null;
        
        InetSocketAddress frameworkNodeAddr1 = NetUtils.createSocketAddr(NNip);			
        LOG.info(" job.getConf().get(\"hdfs.job.jobstutas\")  : " +job.getConf().get("hdfs.job.jobstutas"));	
        if(!job.getConf().get("hdfs.job.jobstutas","true").equals("false")){
	    try {
		      frameworkNode1 = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
				   CProcFrameworkProtocol.versionID, frameworkNodeAddr1, confg,
		           NetUtils.getSocketFactory(confg, CProcFrameworkProtocol.class));
		       frameworkNode1.changeJobEntityType(job.getConf().get("hdfs.job.jobid"), JobEntityType.SEARCH);
	        } catch (IOException e) {
		        e.printStackTrace();
	        }
//	        finally{
//	        	RPC.stopProxy(frameworkNode1);
//	        }
        }
		
		//count++;

		LOG.info("***********************************china_mobile*********************************");
    	LOG.info("*                                                                              *");
    	LOG.info("*                         DoSEARCH is running!!!                                *" + System.currentTimeMillis());
    	LOG.info("*                                                                              *");
    	LOG.info("********************************************************************************");
    	LOG.info("MyJob:" + "cProc.node.port : " + job.getConf().get("cproc.node.port"));
		String message = job.getConf().get("hdfs.job.param.system.param");
		Server.serverURL=job.getConf().get("hdfs.job.param.httpServerIP");
		job.getConf().set("hdfs.job.param.system.param","");
		job.getConf().set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem" );
		Server.ChatProtocolHandler.messageReceived(nameNode,dataNode,message,job.getConf() );
		
		//end
		 job.getConf().set("hdfs.job.param.system.param","");//clear
		 
		LOG.info("++++++++++++++++++++++++++++++DoSEARCH++++++++++++++++++++++++++++++count:" + System.currentTimeMillis());
//		String NNip =	job.getConf().get(Const.NNIP);
//		NNip =	NNip.split(":")[0] + ":" +confg.getInt("cproc.node.port",Const.CONNECTION_PORT) ;
//		LOG.info("NNip  : " +NNip);
		CProcFrameworkProtocol frameworkNode = null;
		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(NNip);			
		LOG.info(" job.getConf().get(\"hdfs.job.jobstutas\")  : " +job.getConf().get("hdfs.job.jobstutas"));	
		if(!job.getConf().get("hdfs.job.jobstutas","true").equals("false")){
			try {
				frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,
						CProcFrameworkProtocol.versionID, frameworkNodeAddr, confg,
				        NetUtils.getSocketFactory(confg, CProcFrameworkProtocol.class));
				LOG.info("hdfs.job.update ------------><-------------------------------------thread id ----"+Thread.currentThread().getId());
				frameworkNode.changeJobEntityType(job.getConf().get("hdfs.job.jobid"), JobEntityType.SUCCESS);
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
//			finally{
//				RPC.stopProxy(frameworkNode);
//			}
		}else
		{
			LOG.info("gyy  nc-----thread id ---"+Thread.currentThread().getId());
		}
		
	
		return null;
		
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
