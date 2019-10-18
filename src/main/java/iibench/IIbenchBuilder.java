package iibench;

public class IIbenchBuilder {
    private String dbName;
    private int writerThreads;
    private Integer maxRows;
    private Integer numDocumentsPerInsert;
    private Integer numInsertsPerFeedback;
    private Integer numSecondsPerFeedback;
    private String logFileName;
    private String compressionType;
    private String writeConcern;
    private String serverName;
    private Integer serverPort;
    private Integer basementSize;
    private Integer numSecondaryIndexes;
    private Integer queryLimit;

    public IIbenchConfig build() {
        return new IIbenchConfig(dbName, writerThreads, maxRows, numDocumentsPerInsert, numInsertsPerFeedback,
                numSecondsPerFeedback, logFileName, compressionType, writeConcern, serverName, serverPort, basementSize,
                numSecondaryIndexes, queryLimit);
    }

    public IIbenchBuilder dbName(final String dbName) {
        this.dbName = dbName;
        return this;
    }

    public IIbenchBuilder writerThreads(final int writerThreads) {
        this.writerThreads = writerThreads;
        return this;
    }

    public IIbenchBuilder numMaxInserts(final Integer maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    public IIbenchBuilder documentsPerInsert(final Integer numDocumentsPerInsert) {
        this.numDocumentsPerInsert = numDocumentsPerInsert;
        return this;
    }

    public IIbenchBuilder insertsPerFeedback(final Integer numInsertsPerFeedback) {
        this.numInsertsPerFeedback = numInsertsPerFeedback;
        return this;
    }

    public IIbenchBuilder secondsPerFeedback(final Integer numSecondsPerFeedback) {
        this.numSecondsPerFeedback = numSecondsPerFeedback;
        return this;
    }

    public IIbenchBuilder logFileName(final String logFileName) {
        this.logFileName = logFileName;
        return this;
    }

    public IIbenchBuilder compressionType(final String compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public IIbenchBuilder writeWriteConcen(final String writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    public IIbenchBuilder serverName(final String serverName) {
        this.serverName = serverName;
        return this;
    }

    public IIbenchBuilder serverPort(final Integer serverPort) {
        this.serverPort = serverPort;
        return this;
    }

    public IIbenchBuilder basementSize(final Integer basementSize) {
        this.basementSize = basementSize;
        return this;
    }

    public IIbenchBuilder numSecondaryIndexes(final Integer numSecondaryIndexes) {
        if ((numSecondaryIndexes < 0) || (numSecondaryIndexes > 3)) {
            throw new UnsupportedOperationException(numSecondaryIndexes + " secondary indexes is not supported");
        }
        this.numSecondaryIndexes = numSecondaryIndexes;
        return this;
    }

    public IIbenchBuilder queryLimit(final Integer queryLimit) {
        this.queryLimit = queryLimit;
        return this;
    }
}