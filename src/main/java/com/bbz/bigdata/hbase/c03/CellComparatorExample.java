package com.bbz.bigdata.hbase.c03;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.xpath.operations.String;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Cell自定义排序功能
 */
public class CellComparatorExample{
    public static void main( String[] args ) throws Exception{
        // vv CellComparatorExample
        Put put1 = new Put( Bytes.toBytes( "row-1" ) );
        put1.addColumn( Bytes.toBytes( "fam-1" ), Bytes.toBytes( "qual-1" ),
                Bytes.toBytes( "val-1" ) );
        Put put2 = new Put( Bytes.toBytes( "row-2" ) );
        put2.addColumn( Bytes.toBytes( "fam-1" ), Bytes.toBytes( "qual-2" ),
                Bytes.toBytes( "val-2" ) );
        Put put3 = new Put( Bytes.toBytes( "row-3" ) );
        put3.addColumn( Bytes.toBytes( "fam-2" ), Bytes.toBytes( "qual-3" ),
                Bytes.toBytes( "val-3" ) );
        Put put4 = new Put( Bytes.toBytes( "row-1" ) );
        put4.addColumn( Bytes.toBytes( "fam-2" ), Bytes.toBytes( "qual-2" ),
                Bytes.toBytes( "val-2" ) );

        CellComparator comparator = new CellComparator.RowComparator();
        List<Cell> cells = new ArrayList<>();

        Put[] puts = {put1, put2, put3, put4};

        for( Put put : puts ) {
            CellScanner scanner = put.cellScanner();
            while( scanner.advance() ) {
                Cell cell = scanner.current();
                cells.add( cell );
            }
        }


        System.out.println( "Shuffling..." );
        Collections.shuffle( cells );
        for( Cell cell : cells ) {
            System.out.println( "Cell: " + cell );
        }

        System.out.println( "Sorting..." );
        Collections.sort( cells, comparator );
        for( Cell cell : cells ) {
            System.out.println( "Cell: " + cell );
        }
        // ^^ CellComparatorExample
    }

}