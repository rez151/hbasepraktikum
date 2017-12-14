package hbasepraktikum;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by reserchr on 12.12.17.
 */
public class Util {

    static List<PerformanceRow> parseTSV(){

        TsvParserSettings settings = new TsvParserSettings();
        //the file used in the example uses '\n' as the line separator sequence.
        //the line separator sequence is defined here to ensure systems such as MacOS and Windows
        //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
        settings.getFormat().setLineSeparator("\n");

        // creates a TSV parser
        TsvParser parser = new TsvParser(settings);

        // parses all rows in one go.
        List<String[]> allRows = null;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("/home/reserchr/IdeaProjects/hbasepraktikum/ressource/performance.tsv"));
            allRows = parser.parseAll(bufferedReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        List<PerformanceRow> performanceRows = new ArrayList<PerformanceRow>();
        for (String[] row : allRows) {
            performanceRows.add(new PerformanceRow(row));
        }

        return performanceRows;
    }
}
