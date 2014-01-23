package org.apache.hadoop.hdfs.job;

import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.hadoop.hdfs.job.Server.Result;

import com.caucho.hessian.client.HessianProxyFactory;

public class SendDataThread implements Runnable {

	public static final HessianProxyFactory factory = new HessianProxyFactory();

	public static ITran tran;
	static {
		try {
			tran = (ITran) factory.create(ITran.class,
					"http://172.3.2.205:8080/tran");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// TODO Auto-generated method stub

	public void run() {
		while (true) {
			try {
				Result result = Server.BLOCK.take();
				sendWithHessian(result.getListValue(), result.getSessionId());
				result = null;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void sendWithHessian(ArrayList listValue, String sessionId) {
		while (true) {
			try {
				if(tran.sendList(listValue, sessionId))
					break;
			} catch (Exception e) {
				e.printStackTrace();
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				continue;
				
			}
		}

	}

}
