package com.bbz.bigdata.hbase.c04;

import com.bbz.bigdata.hbase.util.HBaseHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


// cc DuplicateRegionObserverExample Example of attempting to load the same observer multiple times
// vv DuplicateRegionObserverExample
public class DuplicateRegionObserverExample extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(HRegion.class);

    public static final byte[] FIXED_COLUMN = Bytes.toBytes("@@@GET_COUNTER@@@");
    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
                         Get get, List<Cell> results) throws IOException {
        int count = counter.incrementAndGet();
        LOG.info("Current preGet count: " + count + " [" + this + "]");
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
                          Get get, List<Cell> results) throws IOException {
        Put put = new Put(get.getRow());
        put.addColumn(get.getRow(), FIXED_COLUMN, Bytes.toBytes(counter.get()));
        CellScanner scanner = put.cellScanner();
        scanner.advance();
        Cell cell = scanner.current();
        LOG.debug("Adding fake cell: " + cell);
        results.add(cell);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master-1.internal.larsgeorge.com," +
                "master-2.internal.larsgeorge.com,master-3.internal.larsgeorge.com");
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("testtable");

        HTableDescriptor htd = new HTableDescriptor(tableName)
                .addFamily(new HColumnDescriptor("colfam1"))
                .addCoprocessor(DuplicateRegionObserverExample.class.getCanonicalName(),
                        null, Coprocessor.PRIORITY_USER, null)
                .addCoprocessor(DuplicateRegionObserverExample.class.getCanonicalName(),
                        null, Coprocessor.PRIORITY_USER, null);

        // ^^ DuplicateRegionObserverExample
    /* Does not work as expected! Will throw the following exception:

      Exception in thread "main" java.io.IOException: Coprocessor \
        coprocessor.DuplicateRegionObserverExample already exists.
        at org.apache.hadoop.hbase.HTableDescriptor.addCoprocessor(HTableDescriptor.java:1232)
        at coprocessor.DuplicateRegionObserverExample.main(DuplicateRegionObserverExample.java:69)
        ...
        at com.intellij.rt.execution.application.AppMain.main(AppMain.java:140)
	  */
        // vv DuplicateRegionObserverExample

        Admin admin = connection.getAdmin();
        admin.createTable(htd);
        System.out.println(admin.getTableDescriptor(tableName));

        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 10, "colfam1");

        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("row-1"));
        Result result = table.get(get);

        helper.dumpResult(result);

        table.close();
        admin.close();
        connection.close();
    }
}
// ^^ DuplicateRegionObserverExample