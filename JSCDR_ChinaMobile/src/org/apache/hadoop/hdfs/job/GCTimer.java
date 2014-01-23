package org.apache.hadoop.hdfs.job;

import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;

public class GCTimer {

	
	
	public GCTimer() throws SchedulerException, ParseException
	{
		
		SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
        Scheduler sched = schedFact.getScheduler();
        sched.start();
        JobDetail jobDetail = new JobDetail("QuartzTestJob",
                "QuartzTestJobGroup", GCJob.class);

        jobDetail.getJobDataMap().put("UserName", "Miao Yachun");
        CronTrigger trigger = new CronTrigger("QuartzTestJob",
                "QuartzTestJobGroup");

        //每天一点到6点整点执行一次
        trigger.setCronExpression("0 0 1-6 * * ? ");
        sched.scheduleJob(jobDetail, trigger);
		
	}
	
	
	/**
	 * @param args
	 * @throws ParseException 
	 * @throws SchedulerException 
	 */
	public static void main(String[] args) throws SchedulerException, ParseException {
		// TODO Auto-generated method stub
		 new GCTimer();
		 System.out.println("21212");
	}

}


