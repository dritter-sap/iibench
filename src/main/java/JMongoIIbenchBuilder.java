public class JMongoIIbenchBuilder {
    private String dbName;
    private int writerThreads;
    private Integer maxRows;
    private Integer numDocumentsPerInsert;
    private Integer numInsertsPerFeedback;
    private Integer numSecondsPerFeedback;
    private String logFileName;
    private String compressionType;

    public JMongoIIbenchConfig build() {
        return new JMongoIIbenchConfig(dbName, writerThreads, maxRows, numDocumentsPerInsert, numInsertsPerFeedback, numSecondsPerFeedback, logFileName, compressionType);
    }

    public JMongoIIbenchBuilder dbName(final String dbName) {
        this.dbName = dbName;
        return this;
    }

    public JMongoIIbenchBuilder writerThreads(final int writerThreads) {
        this.writerThreads = writerThreads;
        return this;
    }

    public JMongoIIbenchBuilder numMaxInserts(final Integer maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    public JMongoIIbenchBuilder documentsPerInsert(final Integer numDocumentsPerInsert) {
        this.numDocumentsPerInsert = numDocumentsPerInsert;
        return this;
    }

    public JMongoIIbenchBuilder insertsPerFeedback(final Integer numInsertsPerFeedback) {
        this.numInsertsPerFeedback = numInsertsPerFeedback;
        return this;
    }

    public JMongoIIbenchBuilder secondsPerFeedback(final Integer numSecondsPerFeedback) {
        this.numSecondsPerFeedback = numSecondsPerFeedback;
        return this;
    }

    public JMongoIIbenchBuilder logFileName(final String logFileName) {
        this.logFileName = logFileName;
        return this;
    }

    public JMongoIIbenchBuilder compressionType(final String compressionType) {
        this.compressionType = compressionType;
        return this;
    }
}