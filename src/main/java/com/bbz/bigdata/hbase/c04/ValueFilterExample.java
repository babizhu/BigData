package com.bbz.bigdata.hbase.c04;

import com.bbz.bigdata.hbase.util.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class ValueFilterExample{

    public static void main( String[] args ) throws IOException{
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper( conf );
        helper.dropTable( "testtable" );
        helper.createTable( "testtable", "colfam1", "colfam2" );
        System.out.println( "Adding rows to table..." );
        helper.fillTable( "testtable", 1, 10, 10, "colfam1", "colfam2" );

        Connection connection = ConnectionFactory.createConnection( conf );
        Table table = connection.getTable( TableName.valueOf( "testtable" ) );
        // vv ValueFilterExample
        Filter filter = new ValueFilter( CompareFilter.CompareOp.EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                new SubstringComparator( ".4" ) );

        Scan scan = new Scan();
        scan.setFilter( filter ); // co ValueFilterExample-2-SetFilter Set filter for the scan.
        ResultScanner scanner = table.getScanner( scan );
        // ^^ ValueFilterExample
        System.out.println( "Results of scan:" );
        // vv ValueFilterExample
        for( Result result : scanner ) {
            for( Cell cell : result.rawCells() ) {
                System.out.println( "Cell: " + cell + ", Value: " + // co ValueFilterExample-3-Print1 Print out value to check that filter works.
                        Bytes.toString( cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength() ) );
            }
        }
        scanner.close();

        Get get = new Get( Bytes.toBytes( "row-5" ) );
        get.setFilter( filter ); // co ValueFilterExample-4-SetFilter2 Assign same filter to Get instance.
        Result result = table.get( get );
        // ^^ ValueFilterExample
        System.out.println( "Result of get: " );
        // vv ValueFilterExample
        for( Cell cell : result.rawCells() ) {
            System.out.println( "Cell: " + cell + ", Value: " +
                    Bytes.toString( cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength() ) );
        }
        // ^^ ValueFilterExample

        Filter filter1 = new ValueFilter( CompareFilter.CompareOp.EQUAL, // co ValueFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
                new RegexStringComparator( "val-4.4" ) );

        Scan scan1 = new Scan(  );
        scan1.addColumn( Bytes.toBytes( "colfam1" ), Bytes.toBytes( "col-4" ) );
        scan1.setFilter( filter1 );

        ResultScanner scanner1 = table.getScanner( scan1 );

        System.out.println("自定义的列族-列-值塞选器");
        for( Result r : scanner1 ) {
            for( Cell cell : r.rawCells() ) {
                System.out.println( "Cell: " + cell + ", Value: " + // co ValueFilterExample-3-Print1 Print out value to check that filter works.
                        Bytes.toString( cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength() ) );
            }
        }
        scanner1.close();
//        for (Result r : scanner1) {
//            for (Cell cell : r.rawCells()) {
//                System.out.println("key:"+CellUtil.getCellKeyAsString(cell)+"  "+"value:"+Bytes.toString(CellUtil.cloneValue(cell)));//输出单元格对应的键和值
//            }
//        }
//        for( Cell cell : result.rawCells() ) {
//            System.out.println( "Cell: " + cell + ", Value: " +
//                    Bytes.toString( CellUtil.cloneRow( cell ) ));
//        }

    }
}