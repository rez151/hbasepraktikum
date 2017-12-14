package hbasepraktikum;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HBaseClient {

    private static final String CoprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

    private Connection connection;
    private Admin admin;
    private Configuration config;
    private AggregationClient aggregationClient;


    HBaseClient() {


        config = HBaseConfiguration.create();
        config.addResource("../../../../../INM/Informationssysteme/hbase/hbase-1.2.6/conf/hbase-site.xml");

        try {
            HBaseAdmin.checkHBaseAvailable(config);
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            aggregationClient = new AggregationClient(config);
            System.out.println("connection to hbase successful");
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void createTable(String tableName) {
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                desc.addFamily(new HColumnDescriptor("movies"));
                desc.addCoprocessor(CoprocessorClassName);
                admin.createTable(desc);
                System.out.println("table " + tableName + " created");
            } else {
                System.out.println("table already exists!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void deleteTable(String tableName) {
        Table table;
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = connection.getTable(TableName.valueOf(tableName));
                admin.disableTable(table.getName());
                admin.deleteTable(table.getName());

                System.out.println("table " + tableName + " deleted");
            } else {
                System.out.println("table " + tableName + "doesn't exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertActors(List<PerformanceRow> performanceRows) {
        List<Put> puts = new ArrayList<Put>();

        Table actorTable;

        try {
            if (admin.tableExists(TableName.valueOf("actors"))) {
                actorTable = connection.getTable(TableName.valueOf("actors"));

            } else {
                System.out.println("Table actors doesn't exist!");
                return;
            }

            System.out.println("inserting data...");

            for (PerformanceRow performanceRow : performanceRows) {

                String actor = performanceRow.getActor();
                String movie = performanceRow.getFilm();
                String character = performanceRow.getCharacter();

                Put put = new Put(Bytes.toBytes(actor));
                put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes(movie), Bytes.toBytes(character));
                actorTable.incrementColumnValue(Bytes.toBytes(actor), Bytes.toBytes("movies"), Bytes.toBytes("roles"), 1);

                puts.add(put);
            }

            actorTable.put(puts);

            System.out.println("actors inserted");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long actorRoleCount(String actor) {
        Table table;
        Long result = null;

        try {
            table = connection.getTable(TableName.valueOf("actors"));
            Result getResult = table.get(new Get(Bytes.toBytes(actor)));
            result = Bytes.toLong(getResult.getValue(Bytes.toBytes("movies"), Bytes.toBytes("roles")));

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (result == null) {
            return -1;
        }

        return result;
    }

    public long getMaxRoles() {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("roles"));

        long max = 0;
        try {
            max = aggregationClient.max(TableName.valueOf("actors"), new LongColumnInterpreter(), scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        return max;
    }

    public List<String> getActorByRoles(long value) {

        Table table;
        List<String> results = new ArrayList<String>();
        ResultScanner scanner;
        Scan scan = new Scan();

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("movies"),
                Bytes.toBytes("roles"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(value)
        );

        scan.setFilter(filter);

        try {
            table = connection.getTable(TableName.valueOf("actors"));
            scanner = table.getScanner(scan);

            for (Result row : scanner) {
                String rowKey = Bytes.toString(row.getRow());
                results.add(rowKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;

    }
}
