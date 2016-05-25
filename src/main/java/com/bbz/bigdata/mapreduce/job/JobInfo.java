package com.bbz.bigdata.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.v2.app.job.Job;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by liukun on 16/5/25.
 */
public class JobInfo extends Configured{
    private static final String  HOST = "localhost";

    void a() throws IOException{
        InetSocketAddress jobTracker=new InetSocketAddress(HOST,5000);
        JobClient jobClient = new JobClient( jobTracker,getConf() );

        JobID jobID = new JobID();
        JobStatus[] allJobs = jobClient.getAllJobs();


    }

    void b() throws IOException, InterruptedException{
        Configuration conf = new Configuration();
        Cluster cluster = new Cluster(conf);

        org.apache.hadoop.mapreduce.JobStatus[] allJobStatuses = cluster.getAllJobStatuses();
        if(allJobStatuses != null) {
            for( org.apache.hadoop.mapreduce.JobStatus jobStatuse : allJobStatuses ){


                System.out.println(jobStatuse.getJobID());
                System.out.println(jobStatuse.getJobName());
                System.out.println(jobStatuse.getStartTime());
                System.out.println(jobStatuse.getFinishTime());
                jobStatuse.getMapProgress();

            }
        }
    }



}
