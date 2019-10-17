public class IIbenchBuilder {
    private String dbName;
    private int writerThreads;
    private Integer maxRows;
    private Integer numDocumentsPerInsert;
    private Integer numInsertsPerFeedback;
    private Integer numSecondsPerFeedback;
    private String logFileName;
    private String compressionType;

    public IIbenchConfig build() {
        return new IIbenchConfig(dbName, writerThreads, maxRows, numDocumentsPerInsert, numInsertsPerFeedback, numSecondsPerFeedback, logFileName, compressionType);
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
}