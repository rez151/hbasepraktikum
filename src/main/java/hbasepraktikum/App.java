package hbasepraktikum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        HBaseClient client = new HBaseClient();

        client.createTestData();

        String rowKey = "row0";
        String singleResult = client.getResultByRowKey(rowKey);
        System.out.println("rowKey: " + rowKey);
        System.out.println("result: " + singleResult);


    }
}
