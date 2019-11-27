package iibench;

import iibench.postprocessing.DataSelector;
import iibench.postprocessing.LineResultData;
import iibench.postprocessing.Plotter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchart.XYChart;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostProcessingTest {
    private Plotter plotter;
    private DataSelector ds;

    @Before
    public void setup() throws Exception {
        ds = new DataSelector();
        ds.connect();

        plotter = new Plotter();
    }

    @Test
    public void postProcess_responseTime() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        lrds.add(getLineResultData("sample", "tot_inserts", "elap_secs"));
        lrds.add(getLineResultData("sample2", "tot_inserts", "elap_secs"));

        final XYChart chart = plotter.getXYChart("Sample iiBench (response time)", "Json documents", "Seconds");
        plotter.addSeriesToLineChart(chart, lrds);
        plotter.exportChartAsPDF(chart, "target/sampleIIBench_responseTime");
    }

    @Test
    public void postProcess_insert() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        lrds.add(getLineResultData("sample", "tot_inserts", "int_ips"));
        lrds.add(getLineResultData("sample2", "tot_inserts", "int_ips"));

        final XYChart chart = plotter.getXYChart("Sample iiBench (insert)", "Json documents", "Inserts per Second");
        plotter.addSeriesToLineChart(chart, lrds);
        plotter.exportChartAsPDF(chart, "target/sampleIIBench_insert");
    }

    @Test
    public void postProcess_query() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        lrds.add(getLineResultData("sample", "tot_inserts", "cum_qps"));
        lrds.add(getLineResultData("sample2", "tot_inserts", "cum_qps"));

        final XYChart chart = plotter.getXYChart("Sample iiBench (query)", "Json documents", "Queries per Second");
        plotter.addSeriesToLineChart(chart, lrds);
        plotter.exportChartAsPDF(chart, "target/sampleIIBench_query");
    }

    private LineResultData getLineResultData(final String tableName, final String... fields) throws SQLException {
        final ResultSet resultsSample = queryTimeElapsed(tableName, fields);
        final List<Double> xData = new ArrayList<>();
        final List<Double> yData = new ArrayList<>();
        while (resultsSample.next()) {
            yData.add(resultsSample.getDouble(fields[1]));
            xData.add(resultsSample.getDouble(fields[0]));
        }
        final LineResultData lrd = new LineResultData(tableName);
        lrd.addXData(xData);
        lrd.addYData(yData);
        return lrd;
    }

    private ResultSet queryTimeElapsed(final String tableName, final String... fields) throws SQLException {
        final Statement stmt = ds.getConnection().createStatement();
        return stmt.executeQuery("SELECT " + toCommaSeparatedString(fields) + " FROM " + tableName);
    }

    private String toCommaSeparatedString(final String[] fields) {
        return String.join(",", fields);
    }

    @After
    public void teardown() throws Exception {
        ds.disconnect();
    }
}
