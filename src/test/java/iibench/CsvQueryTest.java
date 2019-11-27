package iibench;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xbib.jdbc.csv.CsvDriver;

import java.sql.*;

public class CsvQueryTest {
    Connection conn;
    @Before
    public void setup() throws Exception {
        Class.forName("org.xbib.jdbc.csv.CsvDriver");
        final String url = "jdbc:xbib:csv:" + "src/test/resources/results?fileExtension=.tsv&separator=\t&suppressHeaders=false&headerline=tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qps\tint_qps\texceptions";
        conn = DriverManager.getConnection(url);
    }

    @Test
    public void queryCsv() throws Exception {
        final Statement stmt = conn.createStatement();
        final ResultSet results = stmt.executeQuery("SELECT tot_inserts,elap_secs FROM sample");
        print(results);
    }

    @After
    public void teardown() throws Exception {
        conn.close();
    }

    private void print(ResultSet results) throws SQLException {
        boolean append = true;
        CsvDriver.writeToCsv(results, System.out, append);
    }
}
