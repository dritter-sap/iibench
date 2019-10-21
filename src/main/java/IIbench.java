import iibench.IIbenchBuilder;
import iibench.IIbenchConfig;
import iibench.databases.DBIIBench;
import iibench.databases.MongoIIBench;
import iibench.databases.OrientIIBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.Writer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class IIbench {
    private static final Logger log = LoggerFactory.getLogger(IIbench.class);

    private static AtomicLong globalInserts = new AtomicLong(0);
    private static AtomicLong globalWriterThreads = new AtomicLong(0);
    private static AtomicLong globalQueryThreads = new AtomicLong(0);
    private static AtomicLong globalQueriesExecuted = new AtomicLong(0);
    private static AtomicLong globalQueriesTimeMs = new AtomicLong(0);
    private static AtomicLong globalQueriesStarted = new AtomicLong(0);
    private static AtomicLong globalInsertExceptions = new AtomicLong(0);

    private static boolean outputHeader = true;
    
    private static int numCashRegisters = 1000;
    private static int numProducts = 10000;
    private static int numCustomers = 100000;
    private static double maxPrice = 500.0;

    private static Integer maxThreadInsertsPerSecond;
    //private static int numCompressibleCharacters;
    //private static int numUncompressibleCharacters;

    private static int randomStringLength = 4*1024*1024;
    private static int compressibleStringLength =  4*1024*1024;
    private static int allDone = 0;

    private final DBIIBench db;

    public IIbench(final DBIIBench db) {
        this.db = db;
    }

    public static void main (String[] args) throws Exception {
        final Properties props = new Properties();
        props.load(IIbench.class.getResourceAsStream("iibench.properties"));
        if (props == null) {
            throw new IllegalArgumentException("'iibench.properties' file not found.");
        }
        final IIbenchConfig config = loadBenchmarkConfig(props);
        final DBIIBench db = new OrientIIBench(config);
        final IIbench iib = new IIbench(db);
        iib.process(props, config);
    }

    private static IIbenchConfig loadBenchmarkConfig(Properties props) {
        return new IIbenchBuilder().dbName(props.getProperty("DB_NAME"))
                .writerThreads(Integer.valueOf(props.getProperty("NUM_LOADER_THREADS")))
                .numMaxInserts(Integer.valueOf(props.getProperty("MAX_ROWS")))
                .documentsPerInsert(Integer.valueOf(props.getProperty("NUM_DOCUMENTS_PER_INSERT")))
                .insertsPerFeedback(Integer.valueOf(props.getProperty("NUM_INSERTS_PER_FEEDBACK")))
                .secondsPerFeedback(Integer.valueOf(props.getProperty("NUM_SECONDS_PER_FEEDBACK")))
                .logFileName(props.getProperty("BENCHMARK_TSV"))
                .compressionType(props.getProperty("COMPRESSION_TYPE"))
                .writeWriteConcen(props.getProperty("WRITE_CONCERN"))
                .serverName(props.getProperty("SERVER_NAME"))
                .serverPort(Integer.valueOf(props.getProperty("SERVER_PORT")))
                .basementSize(Integer.valueOf(props.getProperty("MONGO_BASEMENT")))
                .numSecondaryIndexes(Integer.valueOf(props.getProperty("NUM_SECONDARY_INDEXES")))
                .queryLimit(Integer.valueOf(props.getProperty("QUERY_LIMIT")))
                .numSeconds(Long.valueOf(props.getProperty("RUN_SECONDS")))
                .queryBeginNumDocs(Integer.valueOf(props.getProperty("QUERY_NUM_DOCS_BEGIN")))
                .maxInsertsPerSecond(Integer.valueOf(props.getProperty("MAX_INSERTS_PER_SECOND")))
                .numCharFields(Integer.valueOf(props.getProperty("NUM_CHAR_FIELDS")))
                .lengthCharFields(Integer.valueOf(props.getProperty("LENGTH_CHAR_FIELDS")))
                .percentCompressible(Integer.valueOf(props.getProperty("PERCENT_COMPRESSIBLE")))
                .createCollection(props.get("CREATE_COLLECTION").toString().toLowerCase())
                .queryThreads(Integer.valueOf(props.getProperty("QUERY_THREADS")))
                .msBetweenQueries(Integer.valueOf(props.getProperty("MS_BETWEEN_QUERIES")))
                .queryIndexDirection(Integer.valueOf(props.getProperty("QUERY_DIRECTION")))
        .build();
    }

    public static void logMe(String format, Object... args) {
        System.out.println(Thread.currentThread() + String.format(format, args));
    }

    private void process(final Properties props, final IIbenchConfig config) throws Exception {
        maxThreadInsertsPerSecond = (int) ((double) config.getMaxInsertsPerSecond() / (config.getWriterThreads() > 0 ? config.getWriterThreads() : 1));

        // numCompressibleCharacters = (int) (((double) percentCompressible / 100.0) * (double) config.getLengthCharFields());
        // numUncompressibleCharacters = (int) (((100.0 - (double) percentCompressible) / 100.0) * (double) config.getLengthCharFields());

        this.logSelectedApplicationParameters(config);

        db.connect();
        db.checkIndexUsed();

        log.debug("--------------------------------------------------");

        if (config.getWriterThreads() > 1) {
            config.setMaxRows(config.getMaxRows() / config.getWriterThreads());
        }

        if (config.getCreateCollection().equals("n")) {
            log.debug("Skipping collection creation");
        }
        else
        {
            final String collectionName = "purchases_index";
            db.createCollection(collectionName);
            db.createIndexForCollection();
        }

        try (final Writer writer = new BufferedWriter(new FileWriter(new File(config.getLogFileName())));) {
            final Thread reporterThread = new Thread(this.new ResultToConsoleReporter(config, writer));
            reporterThread.start();

            // start the loaders
            final Thread[] tWriterThreads = new Thread[config.getWriterThreads()];
            for (int i=0; i<config.getWriterThreads(); i++) {
                globalWriterThreads.incrementAndGet();
                tWriterThreads[i] = new Thread(this.new MyWriter(config.getWriterThreads(), i, config.getMaxRows(), db,
                        maxThreadInsertsPerSecond, config.getNumDocumentsPerInsert(), createRandomStringForHolder(),
                        createCompressibleStringHolder(), config));
                tWriterThreads[i].start();
            }

            // start the queryAndMeasureElapsed threads
            Thread[] tQueryThreads = new Thread[config.getQueryThreads()];
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
                    tQueryThreads[i] = new Thread(this.new MyQuery(config.getQueryThreads(), i, db, config.getMsBetweenQueries()));
                    tQueryThreads[i].start();
                }
            }

            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // wait for reporter thread to terminate
            if (reporterThread.isAlive())
                reporterThread.join();

            // wait for queryAndMeasureElapsed thread to terminate
            for (int i=0; i<config.getQueryThreads(); i++) {
                if (tQueryThreads[i].isAlive())
                    tQueryThreads[i].join();
            }

            // wait for writer threads to terminate
            for (int i=0; i<config.getWriterThreads(); i++) {
                if (tWriterThreads[i].isAlive())
                    tWriterThreads[i].join();
            }
        } finally {
            db.disconnect();
            log.debug("Done!");
            System.exit(0);
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

    private void logSelectedApplicationParameters(final IIbenchConfig config) {
        log.debug("Application Parameters");
        log.debug("--------------------------------------------------");
        log.debug("\tdatabase name = {}", config.getDbName());
        log.debug("\t{} writer thread(s)", config.getWriterThreads());
        log.debug("\t{} queryAndMeasureElapsed thread(s)", config.getQueryThreads());
        log.debug("\t{} documents per collection",config.getMaxRows());
        log.debug("\t{} character fields", config.getNumCharFields());
        log.debug("\t{} bytes per character field", config.getLengthCharFields());
        log.debug("\t{} secondary indexes", config.getNumSecondaryIndexes());
        log.debug("\tDocuments Per Insert = {}", config.getNumDocumentsPerInsert());
        log.debug("\tMaximum of {} insert(s) per second", config.getMaxInsertsPerSecond());
        log.debug("\tMaximum of {} insert(s) per second per writer thread", maxThreadInsertsPerSecond);
        log.debug("\tFeedback every {} seconds(s)", config.getNumSecondsPerFeedback());
        log.debug("\tFeedback every {} inserts(s)", config.getNumInsertsPerFeedback());
        log.debug("\tlogging to file {}", config.getLogFileName());
        log.debug("\tRun for {} second(s)", config.getRunSeconds());
        log.debug("\tExtra character fields are {} percent compressible", config.getPercentCompressible());
        log.debug("\t{} milliseconds between queries", config.getMsBetweenQueries());
        log.debug("\tQueries limited to {} document(s) with index direction {}", config.getQueryLimit(), config.getQueryIndexDirection());
        log.debug("\tStarting queries after %{} document(s) inserted", config.getQueryNumDocsBegin());
        log.debug("\twrite concern = {}", config.getWriteConcern());
        log.debug("\tServer:Port = {}:{}", config.getServerName(), config.getServerPort());
    }

    class MyWriter implements Runnable {
        int threadCount; 
        int threadNumber; 
        int numMaxInserts;
        int maxInsertsPerSecond;
        DBIIBench db;
        
        java.util.Random rand;
        private int documentsPerInsert;
        private String randomStringForHolder;
        private String compressibleStringHolder;
        private IIbenchConfig config;

        MyWriter(int threadCount, int threadNumber, int numMaxInserts, DBIIBench db, int maxInsertsPerSecond, int documentsPerInsert,
                 final String randomStringForHolder, final String compressibleStringHolder, final IIbenchConfig config) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.numMaxInserts = numMaxInserts;
            this.maxInsertsPerSecond = maxInsertsPerSecond;
            this.db = db;
            rand = new java.util.Random((long) threadNumber);
            this.documentsPerInsert = documentsPerInsert;
            this.randomStringForHolder = randomStringForHolder;
            this.compressibleStringHolder = compressibleStringHolder;
            this.config = config;
        }
        public void run() {
            // String collectionName = "purchases_index";
            // DBCollection coll = db.getCollection(collectionName);
        
            long numInserts = 0;
            long numLastInserts = 0;
            //int id = 0;
            long nextMs = System.currentTimeMillis() + 1000;
            
            try {
                log.debug("Writer thread {} : started to load collection {}", threadNumber, db.getCollectionName());
                
                int numRounds = numMaxInserts / documentsPerInsert;
                
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
                        //id++;
                        final Map<String, Object> map = new HashMap<>();
                        final int thisCustomerId = rand.nextInt(numCustomers);
                        map.put("dateandtime", System.currentTimeMillis());
                        map.put("cashregisterid", rand.nextInt(numCashRegisters));
                        map.put("customerid", thisCustomerId);
                        map.put("productid", rand.nextInt(numProducts));
                        map.put("price", ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0);
                        docs.add(map);
                    }
                    db.insertDocumentToCollection(docs);

                    try {
                        numInserts += config.getNumDocumentsPerInsert();
                        IIbench.globalInserts.addAndGet(config.getNumDocumentsPerInsert());

                    } catch (Exception e) {
                        //TODO: log.debug("Writer thread {} : EXCEPTION",threadNumber);
                        e.printStackTrace();
                        IIbench.globalInsertExceptions.incrementAndGet();
                    }
                    
                    if (allDone == 1)
                        break;
                }

            } catch (Exception e) {
                log.debug("Writer thread {} : EXCEPTION",threadNumber);
                e.printStackTrace();
            }
            
            long numWriters = globalWriterThreads.decrementAndGet();
            if (numWriters == 0)
                allDone = 1;
        }
    }


    class MyQuery implements Runnable {
        int threadCount; 
        int threadNumber;
        DBIIBench db;

        java.util.Random rand;
        private int msBetweenQueries;

        MyQuery(int threadCount, int threadNumber, DBIIBench db, int msBetweenQueries) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.db = db;
            rand = new java.util.Random((long) threadNumber + globalQueriesStarted.get());
            this.msBetweenQueries = msBetweenQueries;
        }
        public void run() {
            long t0 = System.currentTimeMillis();
            //String collectionName = "purchases_index";
            //DBCollection coll = db.getCollection(collectionName);
        
            long numQueriesExecuted = 0;
            long numQueriesTimeMs = 0;
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
                    int thisProductId = rand.nextInt(numProducts);
                    long thisRandomTime = t0 + (long) ((double) (thisNow - t0) * rand.nextDouble());

                    long elapsed = db.queryAndMeasureElapsed(whichQuery, thisPrice, thisCashRegisterId, thisRandomTime, thisCustomerId);
                    
                    //log.debug("Query thread {} : performing : {}",threadNumber,thisSelect);
                    
                    globalQueriesExecuted.incrementAndGet();
                    globalQueriesTimeMs.addAndGet(elapsed);
                }

            } catch (Exception e) {
                log.debug("Query thread {} : EXCEPTION",threadNumber);
                e.printStackTrace();
            }
            
            long numQueries = globalQueryThreads.decrementAndGet();
        }
    }

    class ResultToConsoleReporter implements Runnable {
        private final IIbenchConfig config;
        private final Writer writer;

        public ResultToConsoleReporter(final IIbenchConfig config, final Writer writer) {
            this.config = config;
            this.writer = writer;
        }

        public void run()
        {
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

            while (allDone == 0)
            {
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
                    
                    if (config.getNumSecondsPerFeedback() > 0) {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f : exceptions=%,d", thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                    } else {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qps=%,.2f : int avg qps=%,.2f : exceptions=%,d", intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                    }
                    
                    try {
                        if (outputHeader)
                        {
                            writer.write("tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qps\tint_qps\texceptions\n");
                            outputHeader = false;
                        }
                            
                        String statusUpdate = "";
                        
                        if (config.getNumSecondsPerFeedback() > 0)
                        {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
                        } else {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS, thisInsertExceptions);
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
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS);
                } else {
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",intervalNumber * config.getNumInsertsPerFeedback(), elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPS, thisIntervalAvgQPS);
                }
                writer.write(statusUpdate);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }
    }
}
