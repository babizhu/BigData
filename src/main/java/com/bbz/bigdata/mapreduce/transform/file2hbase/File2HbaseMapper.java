package com.bbz.bigdata.mapreduce.transform.file2hbase;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liu_k on 2016/5/9.
 */
public class File2HbaseMapper extends Mapper<LongWritable, Text, ObjectUtils.Null, ObjectUtils.Null>{

    private TrackRecordParser parser;
    private Configuration config = null;
    private Connection connection = null ;
    private Table table = null;

    public File2HbaseMapper(  ) throws IOException{

        System.out.println( "run File2HbaseMapper constructor!!!!!!!!!!!!!!!");
    }

    private void addToHbase( TrackRecordParser parse) throws IOException{
        Put p = new Put( Bytes.toBytes( parse.getBadage() + "-" + parse.getTime() ) );

        // To set the value you'd like to update in the row 'myLittleRow', specify
        // the column family, column qualifier, and value of the table cell you'd
        // like to update.  The column family must already exist in your table
        // schema.  The qualifier can be anything.  All must be specified as byte
        // arrays as hbase is all about byte arrays.  Lets pretend the table
        // 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
        p.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "x" ),
                Bytes.toBytes( parse.getX() ) );
        p.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "y" ),
                Bytes.toBytes( parse.getY() ) );
        table.put( p );
    }
    @Override
    protected void setup( Context context ) throws IOException, InterruptedException{
        super.setup( context );
//        this.config = HBaseConfiguration.create();
//        System.out.println( "HBaseConfiguration = \n" + config );
//        this.connection =  ConnectionFactory.createConnection( config );
//        this.table = connection.getTable( TableName.valueOf( "badage" ) );
        parser = new TrackRecordParser();
        System.out.println( "run File2HbaseMapper setUp()!!!!!!!!!!!!!!!");
    }

    @Override
    protected void cleanup( Context context ) throws IOException, InterruptedException{
        super.cleanup( context );
        System.out.println( "File2HbaseMapper.cleanup!!!!!!!!!!!!!!!!" );
        if( table != null ){
            table.close();
        }
        if( connection != null ){
            connection.close();
        }
    }

    public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException{

        parser.parse( value );
//        addToHbase( parser );
        System.out.println( value );

//        context.write( new Text( year ), new IntWritable( airTemperature ) );
    }
}
