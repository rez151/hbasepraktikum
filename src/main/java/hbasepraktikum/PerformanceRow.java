package hbasepraktikum;

/**
 * Created by reserchr on 12.12.17.
 */
public class PerformanceRow {

    private String actor;
    private String film;
    private String character;

    public PerformanceRow(String[] row) {
        this.actor = row[2];
        this.film = row[3];
        this.character = row[5];
    }

    public String getActor() {

        if(this.actor == null)
            return "unnamed";
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public String getFilm() {
        if(this.film == null)
            return "";
        return film;
    }

    public void setFilm(String film) {
        this.film = film;
    }

    public String getCharacter() {
        if(this.character == null)
            return "";
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }
}
