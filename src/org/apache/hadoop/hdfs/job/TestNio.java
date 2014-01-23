package org.apache.hadoop.hdfs.job;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class TestNio {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

//		FileInputStream fileStream = new FileInputStream("q:/2_05.jpg");
//		FileChannel fc = fileStream.getChannel();
//
//		
//		
//		
//		MappedByteBuffer mb = fc.map(MapMode.READ_ONLY, 0, 1000);
//		
//		System.out.println(mb.getClass());
//		
//		DirectByteBufferCleaner.clean(mb);
//
//		
//		
//		
//		fc.close();
//		fileStream.close();

		System.out.println(System.getProperty("os.arch"));
	}

}

final class DirectByteBufferCleaner {
	private DirectByteBufferCleaner() {
	}

	public static void clean(final ByteBuffer byteBuffer) {
		if (!byteBuffer.isDirect())
			return;
		try {
			Object cleaner = invoke(byteBuffer, "cleaner");
			invoke(cleaner, "clean");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Object invoke(final Object target, String methodName)
			throws Exception {
		final Method method = target.getClass().getMethod(methodName);
		method.setAccessible(true);
		return method.invoke(target);

	}

}