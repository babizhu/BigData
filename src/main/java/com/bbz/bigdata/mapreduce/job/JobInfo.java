package com.bbz.bigdata.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Created by liukun on 16/5/25.
 */
public class JobInfo extends Configured{
    private static final String HOST = "master";

    static void a() throws IOException{
        InetSocketAddress jobTracker = new InetSocketAddress( HOST, 9000 );
        JobClient jobClient = new JobClient( new Configuration() );

        JobID jobID = new JobID();
        JobStatus[] allJobs = jobClient.getAllJobs();


    }

    static void b() throws IOException, InterruptedException{

        Thread abcd = new Thread( "abcd" );
        Executors.newSingleThreadExecutor().submit( () -> new Integer(2) );
        abcd.run();
        Configuration conf = new Configuration();
        Cluster cluster = new Cluster( conf );

        TaskTrackerInfo[] activeTaskTrackers = cluster.getActiveTaskTrackers();
        System.out.println( "activeTaskTrackers:");
        for( TaskTrackerInfo taskTracker : activeTaskTrackers ) {
            taskTracker.getTaskTrackerName();
        }
        org.apache.hadoop.mapreduce.JobStatus[] allJobStatuses = cluster.getAllJobStatuses();
        System.out.println( "allJobStatuses:");

        if( allJobStatuses != null ) {
            for( org.apache.hadoop.mapreduce.JobStatus jobStatuse : allJobStatuses ) {


                System.out.println( jobStatuse.getJobID() );
                System.out.println( jobStatuse.getJobName() );
                System.out.println( jobStatuse.getStartTime() );
                System.out.println( jobStatuse.getFinishTime() );
                jobStatuse.getMapProgress();

            }
        }
    }

    public static void main( String[] args ) throws IOException, InterruptedException{
        b();
        a();
    }


}
