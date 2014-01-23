package org.apache.hadoop.hdfs.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dinglicom.decode.bean.BssapCDR;

public class ScanTable {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		BssapCDR cdr = new BssapCDR(1);
		byte[] bytes = new byte[cdr.getSize(1)];
		/**
		 * (assignment_cmp_time>=1 or interbsc_ho_count>=1 or cdr_result==10 or
		 * handover_count>=1) and (end_time_s >= 1354118400 and
		 * end_time_s<=1354121999 ) and cid==1342737648
		 */
		FileStatus[] arr = fs.listStatus(new Path(args[0]));
		for (FileStatus file : arr) {

			FSDataInputStream dis = fs.open(file.getPath());

			while ((dis.read(bytes)) != -1) {
				cdr.decode(bytes);

				if (cdr.get("assign_failure_cause") == 33
						&& (cdr.get("cdr_rel_type") >= 12 || cdr
								.get("cdr_result") == 5)
						&& (cdr.get("end_time_s") >= 1354168800 && cdr
								.get("end_time_s") <= 1354172399)
						&& cdr.get("cid") == 1342734998) {

					System.out.println(file.getPath());

				}
			}

			dis.close();
		}

	}

}
