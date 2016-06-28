package com.bbz.bigdata.mapreduce.hbase;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created by liulaoye on 16-6-27.
 * 一个hbase和mapreduce结合的例子
 * 运行方式:
 * 打包jar上传到服务器，执行
 * hadoop jar ./BigData.jar
 */
public class CountShakespeare{
    private static final byte[] INFO = Bytes.toBytes( "info" );
    private static final byte[] NAME = Bytes.toBytes( "name" );
    private static final String TABLE_NAME = "employee";
    enum COUNTERS{
        ROWS;
    }
    static class Map extends TableMapper<Text,LongWritable>{
        @Override
        protected void map( ImmutableBytesWritable key, Result value, Context context )
                throws IOException, InterruptedException{
            Cell cell = value.getColumnLatestCell( INFO, NAME );
            String name = Bytes.toString( CellUtil.cloneValue( cell ) );
            System.out.println( Bytes.toString( key.get()) + " : " + name );
            if( !Strings.isNullOrEmpty(name)){
                context.getCounter( COUNTERS.ROWS ).increment( 1 );
            }
        }
    }

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance( conf, "hbaseMapReducer" );
        job.setJarByClass( CountShakespeare.class );
        Scan scan = new Scan();
        scan.addColumn( INFO,NAME );

        TableMapReduceUtil.initTableMapperJob(
                TABLE_NAME,
                scan,
                Map.class,
                ImmutableBytesWritable.class,
                Result.class,
                job
                );
        job.setOutputFormatClass( NullOutputFormat.class );
        job.setNumReduceTasks( 0 );
        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }

}
