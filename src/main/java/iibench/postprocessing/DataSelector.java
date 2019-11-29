package iibench.postprocessing;

import org.xbib.jdbc.csv.CsvDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataSelector {
    Connection conn;

    public DataSelector() {
    }

    public void connect(final String path) throws SQLException, ClassNotFoundException {
        Class.forName("org.xbib.jdbc.csv.CsvDriver");
        final String url = "jdbc:xbib:csv:" + path + "?fileExtension=.tsv&separator=\t&suppressHeaders=false&headerline=tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qps\tint_qps\texceptions";
        conn = DriverManager.getConnection(url);
    }

    public void disconnect() throws SQLException {
        conn.close();
    }

    public Connection getConnection() {
        return conn;
    }

    public void printResultSet(final ResultSet results) throws SQLException {
        boolean append = true;
        CsvDriver.writeToCsv(results, System.out, append);
    }
}
