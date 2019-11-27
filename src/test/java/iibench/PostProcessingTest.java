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
    public void postProcess() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        lrds.add(getLineResultData("sample"));
        lrds.add(getLineResultData("sample2"));

        final XYChart chart = plotter.getXYChart("Sample iiBench", "Json documents", "Seconds");
        plotter.addSeriesToLineChart(chart, lrds);
        plotter.exportChartAsPDF(chart, "target/sampleIIBench");
    }

    private LineResultData getLineResultData(final String tableName) throws SQLException {
        final ResultSet resultsSample = queryTimeElapsed(tableName);
        final List<Double> xData = new ArrayList<>();
        final List<Double> yData = new ArrayList<>();
        while (resultsSample.next()) {
            yData.add(resultsSample.getDouble("elap_secs"));
            xData.add(resultsSample.getDouble("tot_inserts"));
        }
        final LineResultData lrd = new LineResultData(tableName);
        lrd.addXData(xData);
        lrd.addYData(yData);
        return lrd;
    }

    private ResultSet queryTimeElapsed(final String tableName) throws SQLException {
        final Statement stmt = ds.getConnection().createStatement();
        return stmt.executeQuery("SELECT tot_inserts,elap_secs FROM " + tableName);
    }

    @After
    public void teardown() throws Exception {
        ds.disconnect();
    }
}
