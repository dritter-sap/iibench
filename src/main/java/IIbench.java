import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import iibench.DBIIBench;
import iibench.DatabaseTypes;
import iibench.IIbenchConfig;
import iibench.threads.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Cli(name = "basic",
        description = "Provides a basic example CLI",
        defaultCommand = IIbench.class,
        commands = { IIbench.class})
@Command(name = "IIbench", description = "Indexed insertion benchmark")
public class IIbench {
    private static final Logger log = LoggerFactory.getLogger(IIbench.class);

    private static AtomicLong globalInserts          = new AtomicLong(0);
    private static AtomicLong globalWriterThreads    = new AtomicLong(0);
    private static AtomicLong globalQueryThreads     = new AtomicLong(0);
    private static AtomicLong globalQueriesExecuted  = new AtomicLong(0);
    private static AtomicLong globalQueriesTimeMs    = new AtomicLong(0);
    private static AtomicLong globalQueriesStarted   = new AtomicLong(0);
    private static AtomicLong globalInsertExceptions = new AtomicLong(0);

    private static boolean outputHeader = true;
    
    private static int numCashRegisters = 1000;
    private static int numProducts      = 10000;
    private static int numCustomers     = 100000;
    private static double maxPrice      = 500.0;

    private static int randomStringLength       = 4*1024*1024;
    private static int compressibleStringLength =  4*1024*1024;

    private static int allDone = 0;

    private static final String IIBENCH_PROPERTIES_FILE_NAME = "iibench.properties";

    @Option(name = { "-user", "--user" }, description = "Database user")
    private String userName = "";

    @Option(name = { "-password", "--password" }, description = "Database password")
    private String password = "";

    @Option(name = { "-host", "--host" }, description = "Database host")
    private String host = "";

    @Option(name = { "-port", "--port" }, description = "Database port")
    private Integer port = 0;

    @Option(name = { "-maxRows", "--maxRows" }, description = "Maximal number of rows inserted during a benchmark")
    private Integer maxRows = 0;

    @Option(name = { "-numDocsPerInsert", "--numDocsPerInsert" }, description = "Number of documents per sinsert (batch size)")
    private Integer numDocsPerInsert = 0;

    @Option(name = { "-queryNumDocsBegin", "--queryNumDocsBegin" }, description = "Start querying after number of documents")
    private Integer queryNumDocsBegin = 0;

    @Option(name = { "-numWriterThreads", "--numWriterThreads" }, description = "Number of writer threads")
    private Integer numWriterThreads = 0;

    @Option(name = { "-numQueryThreads", "--numQueryThreads" }, description = "Number of query threads")
    private Integer numQueryThreads = 0;

    @Option(name = { "-dbType", "--dbType" }, description = "Database types: 'docstore', 'orientdb', 'mongodb', 'mongodbold'")
    private String dbType = "";

    public IIbench() {}

    public static void main (String[] args) {
        final SingleCommand<IIbench> parser = SingleCommand.singleCommand(IIbench.class);
        final IIbench cmd = parser.parse(args);
        cmd.run();
    }

    private void run() {
        final Properties props = new Properties();
        DBIIBench databaseType = null;
        try {
            props.load(IIbench.class.getResourceAsStream(IIBENCH_PROPERTIES_FILE_NAME));
            this.mixinCmdParameterValues(props);
            final IIbenchConfig config = IIbenchConfig.load(props);
            databaseType = DatabaseTypes.select(config.getDbType());
            this.process(databaseType, config, this.userName, this.password);
        } catch (final IOException e) {
            log.error("Failed to load configuration properties " + IIBENCH_PROPERTIES_FILE_NAME, e);
        } catch (final Exception e) {
            log.error("Failed to process benchmark for database type " + databaseType, e);
        }
    }

    public void process(final DBIIBench db, final IIbenchConfig config, final String userName,
                        final String password) throws Exception {
        config.logSelectedApplicationParameters();

        db.connect(config.getServerName(), config.getServerPort(), config.getDbName(), userName, password);
        db.checkIndexUsed();

        log.debug("--------------------------------------------------");
        if (config.getWriterThreads() > 1) {
            config.setMaxRows(config.getMaxRows() / config.getWriterThreads());
        }
        this.createCollectionAndIndex(config, db, "purchases_index");

        try (final Writer writer = new BufferedWriter(new FileWriter(new File(db.getClass().getSimpleName()
                + "-qTs_" + config.getQueryThreads()
                + "-wTs_" + config.getWriterThreads()
                + "-batch_" + config.getNumDocumentsPerInsert()
                + "-qNumDocsBegin_" + config.getQueryNumDocsBegin()
                + "-host_" + config.getServerName()
                + "-"
                + config.getLogFileName())))) {
            final Thread reporterThread = startReporterThread(config, writer);
            final Thread[] writerThreads = startWriterThread(config, db);

            Thread[] queryThreads = null;
            if (config.getMaxRows() > config.getQueryNumDocsBegin()) {
                queryThreads = startQueryThreads(config, db);
            }
            final ThreadUtils threadUtils = new ThreadUtils();
            threadUtils.waitForMs(10000);
            threadUtils.joinThread(reporterThread);
            if (config.getMaxRows() > config.getQueryNumDocsBegin()) {
                threadUtils.joinThreads(queryThreads, config.getQueryThreads());
            }
            threadUtils.joinThreads(writerThreads, config.getWriterThreads());
        } finally {
            db.disconnect(config.getDbName());
            log.debug("Done!");
        }
    }

    private void mixinCmdParameterValues(final Properties props) {
        if (props == null) {
            throw new IllegalArgumentException("'iibench.properties' file not found.");
        }
        if (!this.host.isEmpty()) {
            props.setProperty("SERVER_NAME", this.host);
        }
        if (this.port != 0) {
            props.setProperty("SERVER_PORT", String.valueOf(this.port));
        }
        if (this.maxRows != 0) {
            props.setProperty("MAX_ROWS", String.valueOf(this.maxRows));
        }
        if (this.numDocsPerInsert != 0) {
            props.setProperty("NUM_DOCUMENTS_PER_INSERT", String.valueOf(this.numDocsPerInsert));
        }
        if (this.queryNumDocsBegin != 0) {
            props.setProperty("QUERY_NUM_DOCS_BEGIN", String.valueOf(this.queryNumDocsBegin));
        }
        if (this.numWriterThreads != 0) {
            props.setProperty("NUM_LOADER_THREADS", String.valueOf(this.numWriterThreads));
        }
        if (this.numQueryThreads != 0) {
            props.setProperty("QUERY_THREADS", String.valueOf(this.numQueryThreads));
        }
        if (!this.dbType.isEmpty()) {
            props.setProperty("DB_TYPE", this.dbType);
        }
    }

    private static void logMe(final String format, final Object... args) {
        System.out.println(Thread.currentThread() + String.format(format, args));
    }

    private Thread[] startQueryThreads(final IIbenchConfig config,
                                       final DBIIBench db) {
        final Thread[] tQueryThreads = new Thread[config.getQueryThreads()];
        if (config.getQueryThreads() > 0) {
            if (config.getWriterThreads() > 0) {
                while (globalInserts.get() < config.getQueryNumDocsBegin()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            }
            globalQueriesStarted.set(System.currentTimeMillis());

            for (int i=0; i<config.getQueryThreads(); i++) {
                tQueryThreads[i] = new Thread(this.new MyQuery(config.getQueryThreads(), i, db,
                        config.getMsBetweenQueries(), config.getQueryLimit()));
                tQueryThreads[i].start();
            }
        }
        return tQueryThreads;
    }

    private Thread startReporterThread(final IIbenchConfig config, final Writer writer) {
        final Thread reporterThread = new Thread(this.new ResultToConsoleReporter(config, writer));
        reporterThread.start();
        return reporterThread;
    }

    private Thread[] startWriterThread(final IIbenchConfig config, final DBIIBench db) {
        final Thread[] tWriterThreads = new Thread[config.getWriterThreads()];
        for (int i=0; i<config.getWriterThreads(); i++) {
            globalWriterThreads.incrementAndGet();
            tWriterThreads[i] = new Thread(this.new MyWriter(i, config.getMaxRows(), db,
                    config.getMaxThreadInsertsPerSecond(), config.getNumDocumentsPerInsert(), createRandomStringForHolder(),
                    createCompressibleStringHolder(), config)); // config.getWriterThreads()
            tWriterThreads[i].start();
        }
        return tWriterThreads;
    }

    private void createCollectionAndIndex(final IIbenchConfig config, final DBIIBench db, final String collectionName) {
        if (config.getCreateCollection().equals("n")) {
            log.debug("Skipping collection creation");
        } else {
            db.createCollection(collectionName);
            db.createIndexForCollection();
        }
    }

    private String createCompressibleStringHolder() {
        log.debug("  creating {} bytes of compressible character data...", compressibleStringLength);
        char[] tempStringCompressible = new char[compressibleStringLength];
        for (int i = 0 ; i < compressibleStringLength ; i++) {
            tempStringCompressible[i] = 'a';
        }
        return new String(tempStringCompressible);
    }

    private String createRandomStringForHolder() {
        java.util.Random rand = new java.util.Random();
        log.debug("\tcreating {} bytes of random character data...", randomStringLength);
        final char[] tempString = new char[randomStringLength];
        for (int i = 0 ; i < randomStringLength ; i++) {
            tempString[i] = (char) (rand.nextInt(26) + 'a');
        }
        return new String(tempString);
    }

    /**
     * TODO: refactor from here
     */
    class MyWriter implements Runnable {
        private int threadNumber;
        private int numMaxInserts;
        private int maxInsertsPerSecond;
        private int documentsPerInsert;

        private java.util.Random rand;

        private DBIIBench db;
        private IIbenchConfig config;

        MyWriter(int threadNumber, int numMaxInserts, DBIIBench db, int maxInsertsPerSecond, int documentsPerInsert,
                 final String randomStringForHolder, final String compressibleStringHolder, final IIbenchConfig config) {
            this.threadNumber = threadNumber;
            this.numMaxInserts = numMaxInserts;
            this.maxInsertsPerSecond = maxInsertsPerSecond;

            rand = new java.util.Random((long) threadNumber);
            this.documentsPerInsert = documentsPerInsert;

            this.db = db;
            this.config = config;
        }
        public void run() {
            long numInserts = 0;
            long numLastInserts = 0;
            long nextMs = System.currentTimeMillis() + 1000;
            
            try {
                log.debug("Writer thread {} : started to load collection {}", threadNumber, db.getCollectionName());
                final int numRounds = numMaxInserts / documentsPerInsert;
                
                for (int roundNum = 0; roundNum < numRounds; roundNum++) {
                    if ((numInserts - numLastInserts) >= maxInsertsPerSecond) {
                        // pause until a second has passed
                        while (System.currentTimeMillis() < nextMs) {
                            try {
                                Thread.sleep(20);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        numLastInserts = numInserts;
                        nextMs = System.currentTimeMillis() + 1000;
                    }

                    final List<Map<String, Object>> docs = new ArrayList<>();
                    for (int i = 0; i < config.getNumDocumentsPerInsert(); i++) {
                        final Map<String, Object> map = new HashMap<>();
                        final int thisCustomerId = rand.nextInt(numCustomers);
                        map.put("dateandtime", System.currentTimeMillis());
                        map.put("cashregisterid", rand.nextInt(numCashRegisters));
                        map.put("customerid", thisCustomerId);
                        map.put("productid", rand.nextInt(numProducts));
                        map.put("price", ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0);
                        docs.add(map);
                    }
                    db.insertDocumentToCollection(docs, config.getNumDocumentsPerInsert());

                    try {
                        numInserts += config.getNumDocumentsPerInsert();
                        IIbench.globalInserts.addAndGet(config.getNumDocumentsPerInsert());

                    } catch (final Exception e) {
                        log.debug("Writer thread {} : EXCEPTION", threadNumber, e);
                        IIbench.globalInsertExceptions.incrementAndGet();
                    }
                    if (allDone == 1) {
                        break;
                    }
                }
            } catch (final Exception e) {
                log.debug("Writer thread {} : EXCEPTION", threadNumber, e);
            }
            final long numWriters = globalWriterThreads.decrementAndGet();
            if (numWriters == 0) {
                allDone = 1;
            }
        }
    }

    class MyQuery implements Runnable {
        private int threadCount;
        private int threadNumber;
        private DBIIBench db;

        private java.util.Random rand;
        private int msBetweenQueries;
        private int queryLimit;

        MyQuery(int threadCount, int threadNumber, DBIIBench db, int msBetweenQueries, final int queryLimit) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.db = db;
            rand = new java.util.Random((long) threadNumber + globalQueriesStarted.get());
            this.msBetweenQueries = msBetweenQueries;
            this.queryLimit = queryLimit;
        }

        public void run() {
            long t0 = System.currentTimeMillis();
            int whichQuery = 0;
            
            try {
                log.debug("Query thread {} : ready to queryAndMeasureElapsed collection {}",threadNumber, db.getCollectionName());
                while (allDone == 0) {
                    // wait until my next runtime
                    if (msBetweenQueries > 0) {
                        try {
                            Thread.sleep(msBetweenQueries);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }                    
 
                    long thisNow = System.currentTimeMillis();

                    whichQuery++;
                    if (whichQuery > 3) {
                        whichQuery = 1;
                    }
                            
                    int thisCustomerId = rand.nextInt(numCustomers);
                    double thisPrice = ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0;
                    int thisCashRegisterId = rand.nextInt(numCashRegisters);
                    // int thisProductId = rand.nextInt(numProducts);
                    long thisRandomTime = t0 + (long) ((double) (thisNow - t0) * rand.nextDouble());

                    long elapsed = db.queryAndMeasureElapsed(whichQuery, thisPrice, thisCashRegisterId, thisRandomTime,
                            thisCustomerId, queryLimit);
                    
                    // log.debug("Query thread {} : performing : {}",threadNumber,thisSelect);
                    
                    globalQueriesExecuted.incrementAndGet();
                    globalQueriesTimeMs.addAndGet(elapsed);
                }

            } catch (Exception e) {
                log.debug("Query thread {} : EXCEPTION",threadNumber);
                e.printStackTrace();
            }
            
            // long numQueries = globalQueryThreads.decrementAndGet();
        }
    }

    class ResultToConsoleReporter implements Runnable {
        private final IIbenchConfig config;
        private final Writer writer;

        public ResultToConsoleReporter(final IIbenchConfig config, final Writer writer) {
            this.config = config;
            this.writer = writer;
        }

        public void run() {
            long t0 = System.currentTimeMillis();
            long lastInserts = 0;
            long lastQueriesNum = 0;
            long lastQueriesMs = 0;
            long lastMs = t0;
            long intervalNumber = 0;
            long nextFeedbackMillis = t0 + (1000 * config.getNumSecondsPerFeedback() * (intervalNumber + 1));
            long nextFeedbackInserts = lastInserts + config.getNumInsertsPerFeedback();
            long thisInserts = 0;
            long thisQueriesNum = 0;
            long thisQueriesMs = 0;
            long thisQueriesStarted = 0;
            long endDueToTime = System.currentTimeMillis() + (1000 * config.getRunSeconds());

            while (allDone == 0) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long now = System.currentTimeMillis();
                
                if (now >= endDueToTime)
                {
                    allDone = 1;
                }
                
                thisInserts = globalInserts.get();
                thisQueriesNum = globalQueriesExecuted.get();
                thisQueriesMs = globalQueriesTimeMs.get();
                thisQueriesStarted = globalQueriesStarted.get();
                if (((now > nextFeedbackMillis) && (config.getNumSecondsPerFeedback() > 0)) ||
                    ((thisInserts >= nextFeedbackInserts) && (config.getNumInsertsPerFeedback() > 0)))
                {
                    intervalNumber++;
                    nextFeedbackMillis = t0 + (1000 * config.getNumSecondsPerFeedback() * (intervalNumber + 1));
                    nextFeedbackInserts = (intervalNumber + 1) * config.getNumInsertsPerFeedback();

                    long elapsed = now - t0;
                    long thisIntervalMs = now - lastMs;
                    
                    long thisIntervalInserts = thisInserts - lastInserts;
                    double thisIntervalInsertsPerSecond = thisIntervalInserts/(double)thisIntervalMs*1000.0;
                    double thisInsertsPerSecond = thisInserts/(double)elapsed*1000.0;

                    long thisIntervalQueriesNum = thisQueriesNum - lastQueriesNum;
                    long thisIntervalQueriesMs = thisQueriesMs - lastQueriesMs;
                    double thisIntervalQueryAvgMs = 0;
                    double thisQueryAvgMs = 0;
                    double thisIntervalAvgQPS = 0;
                    double thisAvgQPS = 0;
                    
                    long thisInsertExceptions = globalInsertExceptions.get();

                    if (thisIntervalQueriesNum > 0) {
                        thisIntervalQueryAvgMs = thisIntervalQueriesMs/(double)thisIntervalQueriesNum;
                    }
                    if (thisQueriesNum > 0) {
                        thisQueryAvgMs = thisQueriesMs/(double)thisQueriesNum;
                    }
                    
                    if (thisQueriesStarted > 0) {
                        long adjustedElapsed = now - thisQueriesStarted;
                        if (adjustedElapsed > 0) {
                            thisAvgQPS = (double)thisQueriesNum/((double)adjustedElapsed/1000.0);
                        }
                        if (thisIntervalMs > 0) {
                            thisIntervalAvgQPS = (double)thisIntervalQueriesNum/((double)thisIntervalMs/1000.0);
                        }
                    }
                    
                    if (config.getNumSecondsPerFeedback() > 0) {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f : exceptions=%,d", thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                    } else {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f : exceptions=%,d", intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                    }
                    
                    try {
                        if (outputHeader) {
                            writer.write("tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qps\tint_qps\texceptions\n");
                            outputHeader = false;
                        }
                            
                        String statusUpdate = "";
                        
                        if (config.getNumSecondsPerFeedback() > 0) {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",thisInserts,
                                    elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs,
                                    thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                        } else {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",
                                    intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l,
                                    thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs,
                                    thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                        }
                        writer.write(statusUpdate);
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    lastInserts = thisInserts;
                    lastQueriesNum = thisQueriesNum;
                    lastQueriesMs = thisQueriesMs;

                    lastMs = now;
                }
            }
            
            // output final numbers...
            long now = System.currentTimeMillis();
            thisInserts = globalInserts.get();
            thisQueriesNum = globalQueriesExecuted.get();
            thisQueriesMs = globalQueriesTimeMs.get();
            thisQueriesStarted = globalQueriesStarted.get();
            intervalNumber++;
            // nextFeedbackMillis = t0 + (1000 * config.getNumSecondsPerFeedback() * (intervalNumber + 1));
            // nextFeedbackInserts = (intervalNumber + 1) * config.getNumInsertsPerFeedback();
            long elapsed = now - t0;
            long thisIntervalMs = now - lastMs;
            long thisIntervalInserts = thisInserts - lastInserts;
            double thisIntervalInsertsPerSecond = thisIntervalInserts/(double)thisIntervalMs*1000.0;
            double thisInsertsPerSecond = thisInserts/(double)elapsed*1000.0;
            long thisIntervalQueriesNum = thisQueriesNum - lastQueriesNum;
            long thisIntervalQueriesMs = thisQueriesMs - lastQueriesMs;
            double thisIntervalQueryAvgMs = 0;
            double thisQueryAvgMs = 0;
            double thisIntervalAvgQPS = 0;
            double thisAvgQPS = 0;
            if (thisIntervalQueriesNum > 0) {
                thisIntervalQueryAvgMs = thisIntervalQueriesMs/(double)thisIntervalQueriesNum;
            }
            if (thisQueriesNum > 0) {
                thisQueryAvgMs = thisQueriesMs/(double)thisQueriesNum;
            }
            if (thisQueriesStarted > 0)
            {
                long adjustedElapsed = now - thisQueriesStarted;
                if (adjustedElapsed > 0)
                {
                    thisAvgQPS = (double)thisQueriesNum/((double)adjustedElapsed/1000.0);
                }
                if (thisIntervalMs > 0)
                {
                    thisIntervalAvgQPS = (double)thisIntervalQueriesNum/((double)thisIntervalMs/1000.0);
                }
            }
            if (config.getNumSecondsPerFeedback() > 0)
            {
                logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f", thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS);
            } else {
                logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f", intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS);
            }
            try {
                if (outputHeader)
                {
                    writer.write("tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qps\tint_qps\n");
                    outputHeader = false;
                }
                String statusUpdate = "";
                if (config.getNumSecondsPerFeedback() > 0)
                {
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",thisInserts,
                            elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs,
                            thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS);
                } else {
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",
                            intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond,
                            thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS,
                            thisIntervalAvgQPS);
                }
                writer.write(statusUpdate);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
