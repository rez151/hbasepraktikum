package hbasepraktikum;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by reserchr on 11.12.17.
 */
public class HBaseClient {

    private static final String TABLE_NAME = "test";
    private static final String COLUMN_FAMILY_NAME = "cf1";
    private static final String COLUMN_NAME = "value";

    private static final String[] VALUES = {"value1", "value2", "value3"};

    private Configuration config;
    private Connection connection;
    private Admin admin;

    public HBaseClient() {

        config = HBaseConfiguration.create();

/*        String path = this.getClass()
                .getClassLoader()
                .getResource("../../../../../INM/Informationssysteme/hbase/hbase-1.2.6/conf/hbase-site.xml")//"/home/reserchr/INM/Informationssysteme/hbase/hbase-1.2.6/conf/hbase-site.xml")
                .getPath();*/

        config.addResource("../../../../../INM/Informationssysteme/hbase/hbase-1.2.6/conf/hbase-site.xml");

        System.out.println(config.get("hbase.zookeeper.quorum"));

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


    public void createTable(){
        try {
            if(!admin.tableExists(TableName.valueOf(TABLE_NAME))){
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
                admin.createTable(desc);
                System.out.println("table " + TABLE_NAME + " created");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void createTestData() {

        try {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            System.out.println("Write some greetings to the table");
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

    public String getResultByRowKey(String rowKey) {
        Table table = null;
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
}
