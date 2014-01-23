package org.apache.hadoop.hdfs.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class GCJob implements Job {
	public static final Log LOG = LogFactory.getLog(GCJob.class.getName());
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.gc();
		LOG.info("begin gc================");
	}
}