import iibench.IIbenchBuilder;
import iibench.IIbenchConfig;
import iibench.databases.DBIIBench;
import iibench.databases.MongoIIBench;
import iibench.databases.OrientIIBench;

import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.Writer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class IIbench {
    public static AtomicLong globalInserts = new AtomicLong(0);
    public static AtomicLong globalWriterThreads = new AtomicLong(0);
    public static AtomicLong globalQueryThreads = new AtomicLong(0);
    public static AtomicLong globalQueriesExecuted = new AtomicLong(0);
    public static AtomicLong globalQueriesTimeMs = new AtomicLong(0);
    public static AtomicLong globalQueriesStarted = new AtomicLong(0);
    public static AtomicLong globalInsertExceptions = new AtomicLong(0);

    public static boolean outputHeader = true;
    
    public static int numCashRegisters = 1000;
    public static int numProducts = 10000;
    public static int numCustomers = 100000;
    public static double maxPrice = 500.0;

    public static int queryThreads;
    public static Long numSeconds;
    public static Integer msBetweenQueries;
    public static Integer queryIndexDirection = 1;
    public static Integer queryBeginNumDocs;
    public static Integer maxInsertsPerSecond;
    public static Integer maxThreadInsertsPerSecond;
    public static String serverName;
    public static String createCollection;
    public static int serverPort;
    public static int numCharFields;
    public static int lengthCharFields;
    public static int percentCompressible;
    public static int numCompressibleCharacters;
    public static int numUncompressibleCharacters;

    public static int randomStringLength = 4*1024*1024;
    public static int compressibleStringLength =  4*1024*1024;
    
    public static int allDone = 0;
    private DBIIBench db;

    public IIbench(DBIIBench db) {
        this.db = db;
    }

    public static void main (String[] args) throws Exception {
        final Properties props = new Properties();
        props.load(IIbench.class.getResourceAsStream("iibench.properties"));
        if (props == null) {
            throw new IllegalArgumentException("'iibench.properties' file not found.");
        }
        final IIbenchConfig config = new IIbenchBuilder().dbName(props.getProperty("DB_NAME"))
                .writerThreads(Integer.valueOf(props.getProperty("NUM_LOADER_THREADS"))).numMaxInserts(Integer.valueOf(props.getProperty("MAX_ROWS")))
                .documentsPerInsert(Integer.valueOf(props.getProperty("NUM_DOCUMENTS_PER_INSERT"))).insertsPerFeedback(Integer.valueOf(props.getProperty("NUM_INSERTS_PER_FEEDBACK")))
                .secondsPerFeedback(Integer.valueOf(props.getProperty("NUM_SECONDS_PER_FEEDBACK"))).logFileName(props.getProperty("BENCHMARK_TSV"))
                .compressionType(props.getProperty("COMPRESSION_TYPE")).writeWriteConcen(props.getProperty("WRITE_CONCERN")).serverName(props.getProperty("SERVER_NAME"))
                .serverPort(Integer.valueOf(props.getProperty("SERVER_PORT"))).basementSize(Integer.valueOf(props.getProperty("MONGO_BASEMENT")))
                .numSecondaryIndexes(Integer.valueOf(props.getProperty("NUM_SECONDARY_INDEXES"))).queryLimit(Integer.valueOf(props.getProperty("QUERY_LIMIT"))).build();

        final DBIIBench db = new OrientIIBench(config);
        final IIbench iib = new IIbench(db);
        iib.process(props, config);
    }

    public static void logMe(final String format, final Object... args) {
        System.out.println(Thread.currentThread() + String.format(format, args));
    }

    private void process(final Properties props, final IIbenchConfig config) throws Exception {
        // TODO: add to builder
        numSeconds = Long.valueOf(props.getProperty("RUN_SECONDS"));
        queryBeginNumDocs = Integer.valueOf(props.getProperty("QUERY_NUM_DOCS_BEGIN"));
        maxInsertsPerSecond = Integer.valueOf(props.getProperty("MAX_INSERTS_PER_SECOND"));
        numCharFields = Integer.valueOf(props.getProperty("NUM_CHAR_FIELDS"));
        lengthCharFields = Integer.valueOf(props.getProperty("LENGTH_CHAR_FIELDS"));
        percentCompressible = Integer.valueOf(props.getProperty("PERCENT_COMPRESSIBLE"));
        createCollection = props.get("CREATE_COLLECTION").toString().toLowerCase();
        queryThreads = Integer.valueOf(props.getProperty("QUERY_THREADS"));
        msBetweenQueries = Integer.valueOf(props.getProperty("MS_BETWEEN_QUERIES"));
        queryIndexDirection = Integer.valueOf(props.getProperty("QUERY_DIRECTION"));

        // TODO: inline checks to builder
        if (queryIndexDirection != 1 && queryIndexDirection != -1) {
            logMe("*** ERROR: queryIndexDirection must be 1 or -1 ***");
            System.exit(1);
        }

        maxThreadInsertsPerSecond = (int) ((double)maxInsertsPerSecond / (config.getWriterThreads() > 0 ? config.getWriterThreads() : 1));

        if ((percentCompressible < 0) || (percentCompressible > 100)) {
            logMe("*** ERROR : INVALID PERCENT COMPRESSIBLE, MUST BE >=0 and <= 100 ***");
            logMe("  %d secondary indexes is not supported",percentCompressible);
            System.exit(1);
        }

        numCompressibleCharacters = (int) (((double) percentCompressible / 100.0) * (double) lengthCharFields);
        numUncompressibleCharacters = (int) (((100.0 - (double) percentCompressible) / 100.0) * (double) lengthCharFields);

        this.logSelectedApplicationParameters(config);

        db.connect();
        db.checkIndexUsed();

        logMe("--------------------------------------------------");

        if (config.getWriterThreads() > 1) {
            config.setMaxRows(config.getMaxRows() / config.getWriterThreads());
        }

        if (createCollection.equals("n"))
        {
            logMe("Skipping collection creation");
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
            Thread[] tQueryThreads = new Thread[queryThreads];
            if (queryThreads > 0) {
                if (config.getWriterThreads() > 0) {
                    while (globalInserts.get() < queryBeginNumDocs) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                        }
                    }
                }
                globalQueriesStarted.set(System.currentTimeMillis());

                for (int i=0; i<queryThreads; i++) {
                    tQueryThreads[i] = new Thread(this.new MyQuery(queryThreads, i, db));
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
            for (int i=0; i<queryThreads; i++) {
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
            logMe("Done!");
            System.exit(0);
        }
    }

    private String createCompressibleStringHolder() {
        logMe("  creating %,d bytes of compressible character data...",compressibleStringLength);
        char[] tempStringCompressible = new char[compressibleStringLength];
        for (int i = 0 ; i < compressibleStringLength ; i++) {
            tempStringCompressible[i] = 'a';
        }
        return new String(tempStringCompressible);
    }

    private String createRandomStringForHolder() {
        java.util.Random rand = new java.util.Random();
        logMe("  creating %,d bytes of random character data...",randomStringLength);
        final char[] tempString = new char[randomStringLength];
        for (int i = 0 ; i < randomStringLength ; i++) {
            tempString[i] = (char) (rand.nextInt(26) + 'a');
        }
        return new String(tempString);
    }

    private void logSelectedApplicationParameters(final IIbenchConfig config) {
        logMe("Application Parameters");
        logMe("--------------------------------------------------");
        logMe("  database name = %s", config.getDbName());
        logMe("  %d writer thread(s)", config.getWriterThreads());
        logMe("  %d queryAndMeasureElapsed thread(s)",queryThreads);
        logMe("  %,d documents per collection",config.getMaxRows());
        logMe("  %d character fields",numCharFields);
        logMe("  %d bytes per character field",lengthCharFields);
        logMe("  %d secondary indexes",config.getNumSecondaryIndexes());
        logMe("  Documents Per Insert = %d",config.getNumDocumentsPerInsert());
        logMe("  Maximum of %,d insert(s) per second",maxInsertsPerSecond);
        logMe("  Maximum of %,d insert(s) per second per writer thread",maxThreadInsertsPerSecond);
        logMe("  Feedback every %,d seconds(s)",config.getNumSecondsPerFeedback());
        logMe("  Feedback every %,d inserts(s)",config.getNumInsertsPerFeedback());
        logMe("  logging to file %s",config.getLogFileName());
        logMe("  Run for %,d second(s)",numSeconds);
        logMe("  Extra character fields are %d percent compressible",percentCompressible);
        logMe("  %,d milliseconds between queries", msBetweenQueries);
        logMe("  Queries limited to %,d document(s) with index direction %,d", config.getQueryLimit(), queryIndexDirection);
        logMe("  Starting queries after %,d document(s) inserted",queryBeginNumDocs);
        logMe("  write concern = %s",config.getWriteConcern());
        logMe("  Server:Port = %s:%d",serverName,serverPort);
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
                logMe("Writer thread %d : started to load collection %s", threadNumber, db.getCollectionName());
                
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
                        //TODO: logMe("Writer thread %d : EXCEPTION",threadNumber);
                        e.printStackTrace();
                        IIbench.globalInsertExceptions.incrementAndGet();
                    }
                    
                    if (allDone == 1)
                        break;
                }

            } catch (Exception e) {
                logMe("Writer thread %d : EXCEPTION",threadNumber);
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
        
        MyQuery(int threadCount, int threadNumber, DBIIBench db) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.db = db;
            rand = new java.util.Random((long) threadNumber + globalQueriesStarted.get());
        }
        public void run() {
            long t0 = System.currentTimeMillis();
            //String collectionName = "purchases_index";
            //DBCollection coll = db.getCollection(collectionName);
        
            long numQueriesExecuted = 0;
            long numQueriesTimeMs = 0;
            int whichQuery = 0;
            
            try {
                logMe("Query thread %d : ready to queryAndMeasureElapsed collection %s",threadNumber, db.getCollectionName());
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
                    
                    //logMe("Query thread %d : performing : %s",threadNumber,thisSelect);
                    
                    globalQueriesExecuted.incrementAndGet();
                    globalQueriesTimeMs.addAndGet(elapsed);
                }

            } catch (Exception e) {
                logMe("Query thread %d : EXCEPTION",threadNumber);
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
            long endDueToTime = System.currentTimeMillis() + (1000 * numSeconds);

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
                    
                    if (config.getNumSecondsPerFeedback() > 0)
                    {
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
