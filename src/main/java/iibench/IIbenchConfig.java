package iibench;

public class IIbenchConfig {
    private final String dbName;
    private final int writerThreads;
    private Integer maxRows;
    private final Integer numDocumentsPerInsert;
    private final Integer numInsertsPerFeedback;
    private final Integer numSecondsPerFeedback;
    private final String logFileName;
    private final String compressionType;
    private String writeConcern;
    private final String serverName;
    private final Integer serverPort;
    private Integer basementSize;
    private Integer numSecondaryIndexes;
    private Integer queryLimit;

    public IIbenchConfig(String dbName, int writerThreads, Integer maxRows, Integer numDocumentsPerInsert,
                         Integer numInsertsPerFeedback, Integer numSecondsPerFeedback, String logFileName,
                         String compressionType, String writeConcern, String serverName, Integer serverPort,
                         Integer basementSize, Integer numSecondaryIndexes, Integer queryLimit) {
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
}
