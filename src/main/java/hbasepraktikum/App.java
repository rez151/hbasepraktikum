package hbasepraktikum;

import java.util.List;

public class App {

    private static HBaseClient client = new HBaseClient();

    public static void main(String[] args) {

        List<PerformanceRow> performanceRows = Util.parseTSV();
        client.deleteTable("actors");
        client.createTable("actors");
        client.insertActors(performanceRows);

        long maxRoles = client.getMaxRoles();
        List<String> actors = client.getActorByRoles(maxRoles);

        System.out.println("following actors participated in " + maxRoles + " movies:");
        for (String actor : actors) {
            System.out.println(actor);
        }
    }
}
