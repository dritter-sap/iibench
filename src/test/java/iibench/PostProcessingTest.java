package iibench;

import iibench.postprocessing.DataSelector;
import iibench.postprocessing.LineResultData;
import iibench.postprocessing.Plotter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostProcessingTest {
    private Plotter plotter;
    private DataSelector ds;

    private List<String> tables = new ArrayList<>();

    private static final String dataDirectory = "results/benchmark/simple/test10k"; //"src/test/resources/results";
    private static final String outputDirectory = "results/benchmark/simple/test10k"; //= "target";
    private static final String benchmarkName   = "simple";

    @Before
    public void setup() throws Exception {
        ds = new DataSelector();
        ds.connect(dataDirectory); // "src/test/resources/results"
        plotter = new Plotter();
        Files.newDirectoryStream(Paths.get(dataDirectory), path -> path.toString().endsWith(".tsv"))
                .forEach(d -> tables.add(d.getFileName().toString().substring(0, d.getFileName().toString().lastIndexOf('.'))));
    }

    @Test
    public void postProcess_responseTime() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        getAndAddResultToLineData(lrds, "tot_inserts", "elap_secs");
        final XYChart chart = createAndFillChartToPlotter(lrds, "Sample iiBench (response time)",
                "Json documents", "Seconds", Styler.LegendPosition.InsideNW);
        exportChartAsPDF(chart, "IIBench_responseTime");
    }

    @Test
    public void postProcess_insert() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        getAndAddResultToLineData(lrds, "tot_inserts", "int_ips");
        final XYChart chart = createAndFillChartToPlotter(lrds, "Sample iiBench (insert)",
                "Json documents", "Inserts per Second", Styler.LegendPosition.InsideSW);
        exportChartAsPDF(chart, "IIBench_insert");
    }

    @Test
    public void postProcess_query() throws Exception {
        final List<LineResultData> lrds = new ArrayList<>();
        getAndAddResultToLineData(lrds, "tot_inserts", "cum_qps");
        final XYChart chart = createAndFillChartToPlotter(lrds, "Sample iiBench (query)",
                "Json documents", "Queries per Second", Styler.LegendPosition.InsideNE);
        exportChartAsPDF(chart, "IIBench_query");
    }

    private LineResultData getLineResultData(final String tableName, final String... fields) throws SQLException {
        final long start = System.currentTimeMillis();
        final ResultSet resultsSample = query(tableName, fields);
        System.out.println("Query CSV " + tableName + " (ms): " + (System.currentTimeMillis() - start));
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

    private void exportChartAsPDF(XYChart chart, String iiBench_responseTime) throws IOException {
        final long start = System.currentTimeMillis();
        plotter.exportChartAsPDF(chart, outputDirectory + "/" + benchmarkName + iiBench_responseTime);
        System.out.println("Export PDF " + benchmarkName + " (ms): " + (System.currentTimeMillis() - start));
    }

    private XYChart createAndFillChartToPlotter(final List<LineResultData> lrds, final String chartTitle, final String xAxisTitle,
                                                final String yAxisTitle, final Styler.LegendPosition position) {
        final long start = System.currentTimeMillis();
        final XYChart chart = plotter.getXYChart(chartTitle, xAxisTitle, yAxisTitle, position);
        plotter.addSeriesToLineChart(chart, lrds);
        System.out.println("Create Chart " + chartTitle + " (ms): " + (System.currentTimeMillis() - start));
        return chart;
    }

    private ResultSet query(final String tableName, final String... fields) throws SQLException {
        final Statement stmt = ds.getConnection().createStatement();
        return stmt.executeQuery("SELECT " + toCommaSeparatedString(fields) + " FROM " + tableName);
    }

    private void getAndAddResultToLineData(final List<LineResultData> lrds, final String... fields) throws SQLException {
        for (final String tableName : tables) {
            lrds.add(getLineResultData(tableName, fields));
        }
    }

    private String toCommaSeparatedString(final String[] fields) {
        return String.join(",", fields);
    }

    @After
    public void teardown() throws Exception {
        ds.disconnect();
    }
}
