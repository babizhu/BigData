package com.bbz.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;


/**
 * Created by liu_k on 2016/6/7.
 * hbase最简单的入门级操做例子
 */
public class Guide{

    public static final String TABLE_NAME = "hbase_guid";
    private static final byte[] info = Bytes.toBytes( "info" );
    private static final byte[] age = Bytes.toBytes( "age" );
    private static final byte[] NAME = Bytes.toBytes( "name" );

    private final Configuration config;
    private final Connection connection;
    private final HTable table;
    private final Random random = new Random();

    public Guide() throws IOException{
        this.config = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection( config );
        this.table = (HTable) connection.getTable( TableName.valueOf( TABLE_NAME ) );
    }

    private void singleColumnValueFilter() throws IOException{
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                info,
                age, CompareFilter.CompareOp.GREATER,
                new BinaryComparator( Bytes.toBytes( 98 ) )
        );

//        filter.setFilterIfMissing( true );
        Scan scan = new Scan( );
        scan.setFilter( filter );
        ResultScanner scanner = table.getScanner( scan );
        printResult( scanner );

    }
    /**
     * 写入数据
     */
    public void put() throws IOException{


        for( int i = 0; i < 100; i++ ) {
            Put p = new Put( Bytes.toBytes( i ) );//定义要插入的行键
            p.addColumn( info, NAME, Bytes.toBytes( "bbz" + i ) );
            p.addColumn( info, age, Bytes.toBytes( random.nextInt( 100 ) ) );
            table.put( p );
        }

        //批量写入数据
        List<Put> puts = new ArrayList<>();
        for( int i = 100; i < 200; i++ ) {
            Put p = new Put( Bytes.toBytes( i ) );//定义要插入的行键
            p.addColumn( info, NAME, Bytes.toBytes( "liukun" + i ) );
            p.addColumn( info, age, Bytes.toBytes( random.nextInt( 100 ) ) );
            puts.add( p );
        }

        table.put( puts );

    }

    /**
     * 测试一下相应的原子操作
     */
    private void atomicAction() throws IOException{
        Put p = new Put( Bytes.toBytes( -1 ) );//定义要插入的行键
        p.addColumn( info, NAME, Bytes.toBytes( "liukun" ) );

        boolean result = table.checkAndPut( Bytes.toBytes( -1 ), info, NAME, null, p );
        System.out.println( "插入数据 " + result );
        result = table.checkAndPut( Bytes.toBytes( -1 ), info, NAME, null, p );
        System.out.println( "再次插入数据 " + result );


    }

    private void get() throws IOException{
        int row = random.nextInt( 100 );
        Get get = new Get( Bytes.toBytes( row ) );
        get.setMaxVersions();
//        get.addColumn( info,name );
//        get.addColumn( info,age );
        //屏蔽掉上面两行，hbase会取回所有的列族的值
        Result result = table.get( get );

        byte[] bytes = result.getValue( info, NAME );
        String name = Bytes.toString( bytes );

        bytes = result.getValue( info, age );
        int age = Bytes.toInt( bytes );
        System.out.println( name + "," + age );

        System.out.println( Bytes.toInt( result.value() ) );//返回age的值，因为age列排在name前面（字典序）
        System.out.println( "行键为 :" + Bytes.toInt( result.getRow() ) );
        System.out.println( "size为 ：" + result.size() );
        
        Cell[] cells = result.rawCells();
        for( Cell cell : cells ) {
            System.out.println( cell );
//            System.out.println( Bytes.toString( cell.getFamilyArray()));
        }

        System.out.println( "++++++++++++++++++++++++++++++++" );
        List<Cell> columnCells = result.getColumnCells( info, NAME );
        for( Cell cell : columnCells ) {
            System.out.println( Bytes.toString( CellUtil.cloneValue( cell ) ) );
        }
        System.out.println( "==============把所有的数据打包到一个Map中=============" );
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for( byte[] col : map.keySet() ) {
            System.out.println( Bytes.toString( col ) );
            for( NavigableMap<byte[], NavigableMap<Long, byte[]>> navigableMapNavigableMap : map.values() ) {
                for( Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : navigableMapNavigableMap.entrySet() ) {
                    String colQua = Bytes.toString( entry.getKey() );
                    System.out.println( "\t" + colQua + " : " );
                    for( Map.Entry<Long, byte[]> e : entry.getValue().entrySet() ) {

                        System.out.println( "\t" + e.getKey() + ":" +
                                (colQua.equals( "name" ) ? Bytes.toString( e.getValue() ) : Bytes.toInt( e.getValue() )) );
                    }

                }
            }
        }

        //仅仅获取最新值
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();
        System.out.println( noVersionMap.size() );
    }

    /**
     * scan时候，batch以及cache的组合用法，造成的rpc的变化次数
     */
    private void scanBatch( int caching,int batch, boolean small ) throws IOException{
        int count = 0;
        Scan scan = new Scan()
                .setCaching(caching)  // co ScanCacheBatchExample-1-Set Set caching and batch parameters.
                .setBatch(batch)
                .setSmall(small)
                .setScanMetricsEnabled(true);
        ResultScanner scanner = table.getScanner(scan);
//        printResult( scanner );
        for (Result result : scanner) {
            count++; // co ScanCacheBatchExample-2-Count Count the number of Results available.
        }
        scanner.close();
        ScanMetrics metrics = scan.getScanMetrics();
        System.out.println("Caching: " + caching + ", Batch: " + batch +
                ", Small: " + small + ", Results: " + count +
                ", RPCs: " + metrics.countOfRPCcalls);


    }

    /**
     * 获取数据
     * @throws IOException
     */
    private void scan() throws IOException{
        ResultScanner scanner = null;

        try {
            scanner = table.getScanner( info );

            printResult( scanner );
            System.out.println("==========================");
            Scan scan = new Scan( );
//            scan.setCaching(  )
            scan.addColumn( info,NAME );//特定的列
            scan.setStartRow( Bytes.toBytes( 198 ) );
            scan.setStopRow( Bytes.toBytes( 199 ) );
//            scan.setBatch(  )
            scanner =  table.getScanner( scan );
            printResult( scanner );

            //timeout
            int scannerTimeout = (int) config.getLong( HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1);
            System.out.println( "扫描超时时间为" + scannerTimeout );
        }finally {
            if( scanner != null ){
                scanner.close();
            }
        }


//        table.getScanner( age )

    }

    private void printResult(ResultScanner scanner){
        for( Result result : scanner ) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
            System.out.println(  Bytes.toInt( result.getRow() ));
            for( Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet() ) {

                System.out.println( "\t列族名 : " + Bytes.toString( entry.getKey() ) );
                for( NavigableMap<byte[], byte[]> navigableMap : map.values() ) {
                    for( Map.Entry<byte[], byte[]> entry1 : navigableMap.entrySet() ) {
                        String colQuaName = Bytes.toString( entry1.getKey() );
                        System.out.println( "\t" + colQuaName + " : " +
                                (colQuaName.equals( "name" ) ? Bytes.toString( entry1.getValue() ) : Bytes.toInt( entry1.getValue() )) );
                    }

                }
            }
        }
    }

    /**
     * 杂项测试
     * 1、获取缓冲大小
     * 2、获取表名
     * 3、获取配置内容
     * 4、获取表结构描述
     */
    private void misc() throws IOException{
        BufferedMutator mutator = connection.getBufferedMutator( TableName.valueOf( TABLE_NAME ) );
        System.out.println( "缓冲区长度：" + mutator.getWriteBufferSize() );

        System.out.println( "table name is " + Bytes.toString( table.getTableName()));
        Configuration configuration = table.getConfiguration();
        System.out.println("配置文件内容\n" + configuration);
       this.printTableDescriptor();
    }

    private void printTableDescriptor() throws IOException{
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();


        List<HRegionLocation> regionLocations = table.getRegionLocator().getAllRegionLocations();
        for( HRegionLocation regionLocation : regionLocations ) {
            System.out.println(regionLocation);
        }
        for( HColumnDescriptor columnFamily : columnFamilies ) {
            System.out.println( columnFamily );

            for( Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : columnFamily.getValues().entrySet() ) {
                System.out.println( Bytes.toString(  entry.getKey().get()) + ":" + entry.getValue() );
            }

//            System.out.println(columnFamily.getValues());
        }


    }

    /**
     * 判断行键为5的数据是否存在
     * 判断name为liukun90的数据是否存在
     */
    public void checkExist(){
        Put p = new Put( Bytes.toBytes( 90 ) );//定义要插入的行键
        System.out.println( p.has( info, NAME, Bytes.toBytes( "liukun90" ) ) );
    }

    /**
     * 初始化
     */
    private void init() throws IOException{

    }

    public static void main( String[] args ) throws IOException{
//        new Guide().put();
        Guide guide = new Guide();
//        guide.checkExist();
//        guide.misc();
        guide.singleColumnValueFilter();
//        guide.atomicAction();
//        guide.get();
//        guide.scan();
//        guide.scanBatch( 100,1,false );
    }
}
