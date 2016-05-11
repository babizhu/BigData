package com.bbz.bigdata.mapreduce.transform;

import com.bbz.bigdata.mapreduce.transform.file2hbase.File2Hbase;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by liu_k on 2016/5/9.
 */
public class Launcher{
    public static void main( String[] args ) throws Exception{
        System.out.println( "作业开始运行。。。。。。。。。。");
//        if( args == null || args.length == 0 ){
//            args = new String[2];
//            args[0] = "/input/badage/testlog";
//            args[1] = "/output/badage/";
//        }
        int exitCode = ToolRunner.run( new File2Hbase(), args );
        System.exit( exitCode );
    }
}
