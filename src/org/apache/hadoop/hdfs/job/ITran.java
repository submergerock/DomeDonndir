package org.apache.hadoop.hdfs.job;

import java.util.ArrayList;

public interface ITran {
	
	public boolean sendSize(int size,String sessionId);
	
	public boolean sendList(ArrayList listValue,String sessionId);

}
