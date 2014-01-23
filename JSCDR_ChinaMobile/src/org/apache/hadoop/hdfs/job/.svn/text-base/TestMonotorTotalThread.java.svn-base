package org.apache.hadoop.hdfs.job;

public class TestMonotorTotalThread {

	static
	{
		Thread thread = new Thread(new MonitorTotal());
		thread.start();
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		String sessionId = System.currentTimeMillis()+"";
		
		System.out.println(MonitorTotal.needSend(sessionId, 99999));
		System.out.println(MonitorTotal.needSend(sessionId, 99999));

	}

}
