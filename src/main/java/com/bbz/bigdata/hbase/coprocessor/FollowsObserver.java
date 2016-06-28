package com.bbz.bigdata.hbase.coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created by liulaoye on 16-6-27.
 * 一个hbase协处理器——观察者的例子
 * alter "observ" ,METHOD=>"table_att","coprocessor"=>"file:///home/hadoop/bigdata/BigData.jar|com.bbz.bigdata.hbase.coprocessor.FollowsObserver|1001|"
 *
 */
public class FollowsObserver extends BaseRegionObserver{
    private Table table;
    private static final byte[] PUT_CONTENT = Bytes.toBytes( "putData" );
    private static final byte[] INFO = Bytes.toBytes( "info" );

    @Override
    public void start( CoprocessorEnvironment e ) throws IOException{
        Connection connection = ConnectionFactory.createConnection( e.getConfiguration() );
        table = connection.getTable( TableName.valueOf( "observ" ) );
        super.start( e );
    }

    @Override
    public void stop( CoprocessorEnvironment e ) throws IOException{
        table.close();
    }

    @Override
    public void postPut( ObserverContext<RegionCoprocessorEnvironment> e,
                         Put put, WALEdit edit, Durability durability ) throws IOException{


        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().toString();
        if( tableName.equals( "observ" )){
            return;
        }

        String value = "put内容";
        for( Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet() ) {
            value +=  entry.getKey() + " : " + Bytes.toString( entry.getValue() );
        }
        String key = tableName + (System.currentTimeMillis() + "");

        Put p = new Put( Bytes.toBytes( key ) );
        p.addColumn( INFO, PUT_CONTENT, Bytes.toBytes( value ) );
        this.table.put( p );
        super.postPut( e, put, edit, durability );
    }
}
