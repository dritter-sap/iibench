#!/bin/bash 
# simple script to run against running MongoDB/TokuMX server localhost:(default port)

# if running TokuMX, need to select compression for collection and secondary indexes (zlib is default)
#   valid values : lzma, quicklz, zlib, none
export MONGO_COMPRESSION=quicklz

# if running TokuMX, need to select basement node size (65536 is default)
#   valid values : integer > 0 : 65536 for 64K
# export MONGO_BASEMENT=65536
export MONGO_BASEMENT=16384

# run the benchmark for this many inserts (or the number of minutes defined by RUN_MINUTES)
#   valid values : integer > 0
# export MAX_ROWS=200000000
export MAX_ROWS=$1

# run the benchmark for this many minutes (or the number of inserts defined by MAX_ROWS)
#   valid values : intever > 0
export RUN_MINUTES=$2
export RUN_SECONDS=$[RUN_MINUTES*60]

# total number of documents to insert per "batch"
#   valid values : integer > 0
export NUM_DOCUMENTS_PER_INSERT=$3

# total number of documents to insert per second, allows for the benchmark to be rate limited
#   valid values : integer > 0
export MAX_INSERTS_PER_SECOND=$4

# total number of simultaneous insertion threads
#   valid values : integer > 0
export NUM_LOADER_THREADS=$5

# database in which to run the benchmark
#   valid values : character
export DB_NAME=iibench

# write concern for the benchmark client
#   valid values : FSYNC_SAFE, NONE, NORMAL, REPLICAS_SAFE, SAFE
export WRITE_CONCERN=SAFE

# name of the server to connect to
export MONGO_SERVER=localhost

# port of the server to connect to
export MONGO_PORT=27017

# display performance information every time the client application inserts this many documents
#   valid values : integer > 0, set to -1 if using NUM_SECONDS_PER_FEEDBACK
export NUM_INSERTS_PER_FEEDBACK=-1

# display performance information every time the client application has run for this many seconds
#   valid values : integer > 0, set to -1 if using NUM_INSERTS_PER_FEEDBACK
export NUM_SECONDS_PER_FEEDBACK=10

# number of additional character fields (semi-compressible) to add to each inserted document
#   valid values : integer >= 0
export NUM_CHAR_FIELDS=1

# size (in bytes) of each additional semi-compressible character field
#   valid values : integer >= 0
export LENGTH_CHAR_FIELDS=$6

# percentage of highly compressible data (repeated character "a") in character field
#   valid values : integer >= 0 and <= 100
export PERCENT_COMPRESSIBLE=50

# number of secondary indexes to maintain
#   valid values : integer >= 0 and <= 3
export NUM_SECONDARY_INDEXES=$7

# the following 4 parameters allow an insert plus query workload benchmark

# number of documents to return per query
#   valid values : integer > 0
export QUERY_LIMIT=4

# wait this many inserts to begin the query workload
#   valid values : integer > 0
export QUERY_NUM_DOCS_BEGIN=10000

# number of query threads, must be >= 0
export QUERY_THREADS=$8

# sleep time before a thread starts the next query, must be >= 0
export MS_BETWEEN_QUERIES=0

# create the collection
#   valid values : Y/N
export CREATE_COLLECTION=Y

# 1 = forward, -1 = reverse
export QUERY_DIRECTION=1

JAVA=/usr/local/jdk-8u25-64/bin/java
CLASSPATH=mongo-java-driver-2.13.1.jar
# javac -cp $CLASSPATH:$PWD/src src/jmongoiibench.java

export LOG_NAME=mongoiibench-${MAX_ROWS}-${NUM_DOCUMENTS_PER_INSERT}-${MAX_INSERTS_PER_SECOND}-${NUM_LOADER_THREADS}-${QUERY_THREADS}-${MS_BETWEEN_QUERIES}.txt
export BENCHMARK_TSV=${LOG_NAME}.tsv
    
rm -f $LOG_NAME
rm -f $BENCHMARK_TSV

iostat -kx 10 >& o.io.r &
iid=$!
vmstat 10 >& o.vm.r &
vid=$!

# python mstat.py --loops 1000000 --interval 10 >& o.mstat.r &
# mid=$!

T="$(date +%s)"
java -cp target/iibench.jar iibench.IIbench $DB_NAME $NUM_LOADER_THREADS $MAX_ROWS $NUM_DOCUMENTS_PER_INSERT $NUM_INSERTS_PER_FEEDBACK $NUM_SECONDS_PER_FEEDBACK $BENCHMARK_TSV $MONGO_COMPRESSION $MONGO_BASEMENT $RUN_SECONDS $QUERY_LIMIT $QUERY_NUM_DOCS_BEGIN $MAX_INSERTS_PER_SECOND $WRITE_CONCERN $MONGO_SERVER $MONGO_PORT $NUM_CHAR_FIELDS $LENGTH_CHAR_FIELDS $NUM_SECONDARY_INDEXES $PERCENT_COMPRESSIBLE $CREATE_COLLECTION $QUERY_THREADS $MS_BETWEEN_QUERIES $QUERY_DIRECTION 2>&1 | tee -a $LOG_NAME
echo "" | tee -a $LOG_NAME
T="$(($(date +%s)-T))"
printf "`date` | iibench duration = %02d:%02d:%02d:%02d\n" "$((T/86400))" "$((T/3600%24))" "$((T/60%60))" "$((T%60))" | tee -a $LOG_NAME

export mdir=$9
echo "db.serverStatus()" | ${mdir}/bin/mongo > o.status
du -hs ${mdir}/data >> $LOG_NAME
du -hs --apparent-size ${mdir}/data >> $LOG_NAME
ps aux | grep mongod | grep -v grep >> $LOG_NAME
tail -3 $LOG_NAME

# kill $mid
kill $iid
kill $vid
