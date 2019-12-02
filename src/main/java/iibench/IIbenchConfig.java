package iibench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class IIbenchConfig {
    private static final Logger log = LoggerFactory.getLogger(IIbenchConfig.class);

    private final String  dbName;
    private final int     writerThreads;
    private Integer       maxRows;
    private final Integer numDocumentsPerInsert;
    private final Integer numInsertsPerFeedback;
    private final Integer numSecondsPerFeedback;
    private final String  logFileName;
    private final String  compressionType;
    private String        writeConcern;
    private final String  serverName;
    private final Integer serverPort;
    private Integer       basementSize;
    private Integer       numSecondaryIndexes;
    private Integer       queryLimit;
    private Long          runSeconds;
    private Integer       queryNumDocsBegin;
    private Integer       maxInsertsPerSecond;
    private Integer       numCharFields;
    private Integer       lengthCharFields;
    private Integer       percentCompressible;
    private String        createCollection;
    private Integer       queryThreads;
    private Integer       msBetweenQueries;
    private Integer       queryIndexDirection;
    private Integer       maxThreadInsertsPerSecond;
    private String        dbType;
    private Boolean       withIndex;
    private String        dataGenType;

    public IIbenchConfig(String dbName, int writerThreads, Integer maxRows, Integer numDocumentsPerInsert,
                         Integer numInsertsPerFeedback, Integer numSecondsPerFeedback, String logFileName,
                         String compressionType, String writeConcern, String serverName, Integer serverPort,
                         Integer basementSize, Integer numSecondaryIndexes, Integer queryLimit, Long runSeconds,
                         Integer queryNumDocsBegin, Integer maxInsertsPerSecond, Integer numCharFields,
                         Integer lengthCharFields, Integer percentCompressible, String createCollection,
                         Integer queryThreads, Integer msBetweenQueries, Integer queryIndexDirection, String dbType,
                         Boolean withIndex, String dataGenType) {
        this.dbName = dbName;
        this.writerThreads = writerThreads;
        this.maxRows = maxRows;
        this.numDocumentsPerInsert = numDocumentsPerInsert;
        this.numInsertsPerFeedback = numInsertsPerFeedback;
        this.numSecondsPerFeedback = numSecondsPerFeedback;
        this.logFileName = logFileName;
        this.compressionType = compressionType;
        this.writeConcern = writeConcern;
        this.serverName = serverName;
        this.serverPort = serverPort;
        this.basementSize = basementSize;
        this.numSecondaryIndexes = numSecondaryIndexes;
        this.queryLimit = queryLimit;
        this.runSeconds = runSeconds;
        this.queryNumDocsBegin = queryNumDocsBegin;
        this.maxInsertsPerSecond = maxInsertsPerSecond;
        this.numCharFields = numCharFields;
        this.lengthCharFields = lengthCharFields;
        this.percentCompressible = percentCompressible;
        this.createCollection = createCollection;
        this.queryThreads = queryThreads;
        this.msBetweenQueries = msBetweenQueries;
        this.queryIndexDirection = queryIndexDirection;
        this.dbType = dbType;
        this.withIndex = withIndex;
        this.dataGenType = dataGenType;
    }

    public static IIbenchConfig load(final Properties props) {
        final IIbenchConfig config = new IIbenchBuilder().dbName(props.getProperty("DB_NAME"))
                .writerThreads(Integer.valueOf(props.getProperty("NUM_LOADER_THREADS")))
                .numMaxInserts(Integer.valueOf(props.getProperty("MAX_ROWS")))
                .documentsPerInsert(Integer.valueOf(props.getProperty("NUM_DOCUMENTS_PER_INSERT")))
                .insertsPerFeedback(Integer.valueOf(props.getProperty("NUM_INSERTS_PER_FEEDBACK")))
                .secondsPerFeedback(Integer.valueOf(props.getProperty("NUM_SECONDS_PER_FEEDBACK")))
                .logFileName(props.getProperty("BENCHMARK_TSV"))
                //.compressionType(props.getProperty("COMPRESSION_TYPE"))
                .writeWriteConcen(props.getProperty("WRITE_CONCERN"))
                .serverName(props.getProperty("SERVER_NAME"))
                .serverPort(Integer.valueOf(props.getProperty("SERVER_PORT")))
                //.basementSize(Integer.valueOf(props.getProperty("MONGO_BASEMENT")))
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
                .dbType(props.getProperty("DB_TYPE"))
                .withIndex(Boolean.parseBoolean(props.getProperty("WITH_INDEX")))
                .dataGenType(props.getProperty("DATA_GEN_TYPE")).build();
        config.setMaxThreadInsertsPerSecond((int) ((double) config.getMaxInsertsPerSecond() / (config.getWriterThreads() > 0 ? config.getWriterThreads() : 1)));
        return config;
    }

    public void logSelectedApplicationParameters() {
        log.debug("Application Parameters");
        log.debug("--------------------------------------------------");
        log.debug("\tdatabase name = {}", this.getDbName());
        log.debug("\t{} writer thread(s)", this.getWriterThreads());
        log.debug("\t{} queryAndMeasureElapsed thread(s)", this.getQueryThreads());
        log.debug("\t{} documents per collection",this.getMaxRows());
        log.debug("\t{} character fields", this.getNumCharFields());
        log.debug("\t{} bytes per character field", this.getLengthCharFields());
        log.debug("\t{} secondary indexes", this.getNumSecondaryIndexes());
        log.debug("\tDocuments Per Insert = {}", this.getNumDocumentsPerInsert());
        log.debug("\tMaximum of {} insert(s) per second", this.getMaxInsertsPerSecond());
        log.debug("\tMaximum of {} insert(s) per second per writer thread", this.getMaxThreadInsertsPerSecond());
        log.debug("\tFeedback every {} seconds(s)", this.getNumSecondsPerFeedback());
        log.debug("\tFeedback every {} inserts(s)", this.getNumInsertsPerFeedback());
        log.debug("\tlogging to file {}", this.getLogFileName());
        log.debug("\tRun for {} second(s)", this.getRunSeconds());
        log.debug("\tExtra character fields are {} percent compressible", this.getPercentCompressible());
        log.debug("\t{} milliseconds between queries", this.getMsBetweenQueries());
        log.debug("\tQueries limited to {} document(s) with index direction {}", this.getQueryLimit(), this.getQueryIndexDirection());
        log.debug("\tStarting queries after %{} document(s) inserted", this.getQueryNumDocsBegin());
        log.debug("\twrite concern = {}", this.getWriteConcern());
        log.debug("\tServer:Port = {}:{}", this.getServerName(), this.getServerPort());
    }

    public void setMaxThreadInsertsPerSecond(final Integer maxThreadInsertsPerSecond) {
        this.maxThreadInsertsPerSecond = maxThreadInsertsPerSecond;
    }

    public String getDbName() {
        return dbName;
    }

    public int getWriterThreads() {
        return writerThreads;
    }

    public Integer getMaxRows() {
        return maxRows;
    }

    public Integer getNumDocumentsPerInsert() {
        return numDocumentsPerInsert;
    }

    public Integer getNumInsertsPerFeedback() {
        return numInsertsPerFeedback;
    }

    public Integer getNumSecondsPerFeedback() {
        return numSecondsPerFeedback;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setMaxRows(final Integer maxRows) {
        this.maxRows = maxRows;
    }

    public String getWriteConcern() {
        return writeConcern;
    }

    public String getServerName() {
        return serverName;
    }

    public Integer getServerPort() {
        return serverPort;
    }

    public Integer getBasementSize() {
        return basementSize;
    }

    public Integer getNumSecondaryIndexes() {
        return numSecondaryIndexes;
    }

    public Integer getQueryLimit() {
        return queryLimit;
    }

    public Long getRunSeconds() {
        return runSeconds;
    }

    public Integer getQueryNumDocsBegin() {
        return queryNumDocsBegin;
    }

    public Integer getMaxInsertsPerSecond() {
        return maxInsertsPerSecond;
    }

    public Integer getNumCharFields() {
        return numCharFields;
    }

    public Integer getLengthCharFields() {
        return lengthCharFields;
    }

    public Integer getPercentCompressible() {
        return percentCompressible;
    }

    public String getCreateCollection() {
        return createCollection;
    }

    public Integer getQueryThreads() {
        return queryThreads;
    }

    public Integer getMsBetweenQueries() {
        return msBetweenQueries;
    }

    public Integer getQueryIndexDirection() {
        return queryIndexDirection;
    }

    public Integer getMaxThreadInsertsPerSecond() {
        return maxThreadInsertsPerSecond;
    }

    public String getDbType() {
        return dbType;
    }

    public Boolean getWithIndex() {
        return withIndex;
    }

    public String getDataGenType() {
        return dataGenType;
    }
}
