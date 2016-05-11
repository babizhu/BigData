package com.bbz.bigdata.mapreduce.transform.file2hbase;

import com.bbz.bigdata.mapreduce.util.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * Created by liu_k on 2016/5/9.
 * 把hadoop的文件从文件通过Map Reduce传输到hbase中
 * 在本例中，要转换的文件是一个胸牌产生的轨迹文件，格式如下
 * 0000021B5861A3B4687B2EC879B0AA16	2000	309	0	3075E3CF42D04810B93B20955406655D	1971	2016-01-19 00:32:58	d1311bb824ec	192.168.1.119,1258.9254117941675,-81;192.168.1.115,1995.2623149688798,-85
 */
public class File2Hbase extends Configured implements Tool{

    @Override
    public int run( String[] args ) throws Exception{
        Job job = JobBuilder.parseInputAndOutput( this, getConf(), args );
        if( job == null ) {
            return -1;
        }
//        job.setInputFormatClass( WholeFileInputFormat.class );
//        job.setOutputFormatClass( SequenceFileOutputFormat.class );
//        job.setOutputKeyClass( IntWritable.class );
//        job.setOutputValueClass( IntWritable.class );
        job.setMapperClass( File2HbaseMapper.class );
//        job.setReducerClass( File2HbaseReducer.class );
        return job.waitForCompletion( true ) ? 0 : 1;
    }
}
