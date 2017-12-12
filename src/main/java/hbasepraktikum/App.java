package hbasepraktikum;

import java.util.Map;

public class App {

    private static HBaseClient client = new HBaseClient();

    public static void main(String[] args) {

        client.createTable();
        client.createTestData();

        singleResult();
        manyResult();

        client.deleteTable();
    }

    private static void manyResult() {
        Map<String,String> results = client.getResultsFromTable();

        System.out.println("results:");
        for (Map.Entry<String, String> entry : results.entrySet()) {
            System.out.println("key: " + entry.getKey() + "  value: " + entry.getValue());
        }

    }

    private static void singleResult() {

        String rowKey = "row0";
        String singleResult = client.getResultByRowKey(rowKey);
        System.out.println("rowKey: " + rowKey);
        System.out.println("result: " + singleResult);
    }
}
