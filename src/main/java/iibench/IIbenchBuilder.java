package iibench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IIbenchBuilder {
    private static final Logger log = LoggerFactory.getLogger(IIbenchBuilder.class);

    private String  dbName;
    private int     writerThreads;
    private Integer maxRows;
    private Integer numDocumentsPerInsert;
    private Integer numInsertsPerFeedback;
    private Integer numSecondsPerFeedback;
    private String  logFileName;
    private String  compressionType;
    private String  writeConcern;
    private String  serverName;
    private Integer serverPort;
    private Integer basementSize;
    private Integer numSecondaryIndexes;
    private Integer queryLimit;
    private Long    runSeconds;
    private Integer queryNumDocsBegin;
    private Integer maxInsertsPerSecond;
    private Integer numCharFields;
    private Integer lengthCharFields;
    private Integer percentCompressible;
    private String  createCollection;
    private Integer queryThreads;
    private Integer msBetweenQueries;
    private Integer queryIndexDirection;
    private String  dbType;
    private boolean withIndex;
    private String  dataGenType;

    public IIbenchConfig build() {
        return new IIbenchConfig(dbName, writerThreads, maxRows, numDocumentsPerInsert, numInsertsPerFeedback,
                numSecondsPerFeedback, logFileName, compressionType, writeConcern, serverName, serverPort, basementSize,
                numSecondaryIndexes, queryLimit, runSeconds, queryNumDocsBegin, maxInsertsPerSecond, numCharFields,
                lengthCharFields, percentCompressible, createCollection, queryThreads, msBetweenQueries, queryIndexDirection,
                dbType, withIndex, dataGenType);
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

    @Deprecated
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

    @Deprecated
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

    public IIbenchBuilder numSeconds(final Long runSeconds) {
        this.runSeconds = runSeconds;
        return this;
    }

    public IIbenchBuilder queryBeginNumDocs(final Integer queryNumDocsBegin) {
        this.queryNumDocsBegin = queryNumDocsBegin;
        return this;
    }

    public IIbenchBuilder maxInsertsPerSecond(final Integer maxInsertsPerSecond) {
        this.maxInsertsPerSecond = maxInsertsPerSecond;
        return this;
    }

    public IIbenchBuilder numCharFields(final Integer numCharFields) {
        this.numCharFields = numCharFields;
        return this;
    }

    public IIbenchBuilder lengthCharFields(final Integer lengthCharFields) {
        this.lengthCharFields = lengthCharFields;
        return this;
    }

    public IIbenchBuilder percentCompressible(final Integer percentCompressible) {
        if ((percentCompressible < 0) || (percentCompressible > 100)) {
            throw new IllegalArgumentException("*** ERROR : INVALID PERCENT COMPRESSIBLE, MUST BE >=0 and <= 100 *** \n " +
                    percentCompressible + " secondary indexes is not supported");
        }
        this.percentCompressible = percentCompressible;
        return this;
    }

    public IIbenchBuilder createCollection(final String createCollection) {
        this.createCollection = createCollection;
        return this;
    }

    public IIbenchBuilder queryThreads(final Integer queryThreads) {
        this.queryThreads = queryThreads;
        return this;
    }

    public IIbenchBuilder msBetweenQueries(final Integer msBetweenQueries) {
        this.msBetweenQueries = msBetweenQueries;
        return this;
    }

    public IIbenchBuilder queryIndexDirection(Integer queryIndexDirection) {
        if (queryIndexDirection != 1 && queryIndexDirection != -1) {
            queryIndexDirection = 1; // default
            log.debug("*** ERROR: queryIndexDirection must be 1 or -1 ***");
        }
        this.queryIndexDirection = queryIndexDirection;
        return this;
    }

    public IIbenchBuilder dbType(final String dbType) {
        this.dbType = dbType;
        return this;
    }

    public IIbenchBuilder withIndex(final boolean withIndex) {
        this.withIndex = withIndex;
        return this;
    }

    public IIbenchBuilder dataGenType(final String dataGenType) {
        this.dataGenType = dataGenType;
        return this;
    }
}