package hbasepraktikum;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HBaseClient {

    private static final String TABLE_NAME = "test";
    private static final String COLUMN_FAMILY_NAME = "cf1";
    private static final String COLUMN_NAME = "value";

    private static final String[] VALUES = {"value1", "value2", "value3"};

    private Connection connection;
    private Admin admin;

    HBaseClient() {

        Configuration config = HBaseConfiguration.create();

        config.addResource("../../../../../INM/Informationssysteme/hbase/hbase-1.2.6/conf/hbase-site.xml");

        try {
            HBaseAdmin.checkHBaseAvailable(config);
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
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
                admin.createTable(desc);
                System.out.println("table " + tableName + " created");
            }else {
                System.out.println("table already exists!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void createTestData() {

        try {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            System.out.println("Write some values to the table");
            for (int i = 0; i < VALUES.length; i++) {
                String rowKey = "row" + i;

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME), Bytes.toBytes(VALUES[i]));
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String getResultByRowKey(String rowKey) {
        Table table;
        String result = null;

        try {
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
            result = Bytes.toString(getResult.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME)));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    Map<String, String> getResultsFromTable() {
        Table table;
        ResultScanner scanner;
        Map<String, String> results = new HashMap<String, String>();
        Scan scan = new Scan();

        System.out.println("Scan for all values:");
        try {
            if(!admin.tableExists(TableName.valueOf(TABLE_NAME))){
                System.out.println("table " + TABLE_NAME + " doesn't exist!");
                return null;
            }

            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            scanner = table.getScanner(scan);
            for (Result row : scanner) {
                String rowKey = Bytes.toString(row.getRow());
                byte[] valueBytes = row.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME));
                String value = Bytes.toString(valueBytes);
                results.put(rowKey, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;
    }

    void deleteTable(String tableName) {
        Table table;
        try {
            if(admin.tableExists(TableName.valueOf(tableName))) {
                table = connection.getTable(TableName.valueOf(tableName));
                admin.disableTable(table.getName());
                admin.deleteTable(table.getName());

                System.out.println("Table " + tableName + " deleted successfully!");
            }else {
                System.out.println("Table " + tableName + "doesn't exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertActors(List<PerformanceRow> performanceRows) {
        String columnFamily = "actors";
        List<Put> puts = new ArrayList<Put>();

        Table actorTable = null;

            try {
                if (admin.tableExists(TableName.valueOf("actors"))){
                    actorTable = connection.getTable(TableName.valueOf("actors"));

                }else{
                    System.out.println("Table actors doesn't exist!");
                    return;
                }


                for (PerformanceRow performanceRow : performanceRows) {

                    String actor = performanceRow.getActor();
                    String movie = performanceRow.getFilm();
                    String character = performanceRow.getCharacter();

                    Put put = new Put(Bytes.toBytes(actor));
                    put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes(movie), Bytes.toBytes(character));
                    //TODO: irgendwie incrementieren
                    put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("roles"), Bytes.toBytes(0));
                    puts.add(put);
                }

                actorTable.put(puts);

            } catch (IOException e) {
                e.printStackTrace();
            }




    }
}
