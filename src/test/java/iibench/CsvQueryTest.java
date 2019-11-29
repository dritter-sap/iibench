package iibench;

import iibench.postprocessing.DataSelector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class CsvQueryTest {
    private DataSelector ds;

    @Before
    public void setup() throws Exception {
        ds = new DataSelector();
        ds.connect("src/test/resources/results");
    }

    @Test
    public void queryCsv() throws Exception {
        final Statement stmt = ds.getConnection().createStatement();
        final ResultSet results = stmt.executeQuery("SELECT tot_inserts,elap_secs FROM sample");
        ds.printResultSet(results);
    }

    @After
    public void teardown() throws Exception {
        ds.disconnect();
    }
}
