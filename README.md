iibench-mongodb
===============

Indexed insertion Benchmark (iiBench) for MongoDB and OrientDB. The original benchmark was developed by Percona (cf. https://github.com/tmcallaghan/iibench-mongodb), however, last updated in 2014.

Contributions:
* fixed bugs (e.g., threading problems)
* ported benchmark to mongodb v3.11.1
* added a benchmark for orientdb v3.0.24


Requirements
=====================

* Java 1.8+
<!---* The MongoDB Java driver must exist and be in the CLASSPATH, as in "export CLASSPATH=/home/tcallaghan/java_goodies/mongo-2.11.4.jar:.". If you don't already have the MongoDB Java driver, then execute the following two commands:

```bash
wget http://central.maven.org/maven2/org/mongodb/mongo-java-driver/2.11.4/mongo-java-driver-2.11.4.jar
export CLASSPATH=$PWD/mongo-java-driver-2.11.4.jar:$CLASSPATH

```

* This example assumes that you already have a MongoDB server running on the same machine as the iiBench client application.
* You can connect a different server or port by editing the run.simple.bash script. -->


Running the benchmark
=====================

<!---#In the default configuration the benchmark will run for 1 hour, or 100 million inserts, whichever comes first.-->

```bash
git clone https://github.com/dritter-sap/iibench-mongodb.git
cd iibench-mongodb
mvn clean package

```

<!---*[optionally edit run.simple.bash to modify the benchmark behavior]*-->

*OrientDB*
```bash 
[cd target]

java -jar iibench.jar -host plocal:localhost -user root -password <password> -maxRows 1000000 -numDocsPerInsert 1000 -queryNumDocsBegin 1000000 -numWriterThreads 1 -numQueryThreads 1 -dbType -dbType orientdb

```

*MongoDB*
```bash
[cd target]

java -jar iibench.jar -host localhost -port 27017 -user root -password <password> -maxRows 1000000 -numDocsPerInsert 1000 -queryNumDocsBegin 100000 -numWriterThreads 1 -numQueryThreads 1 -dbType mongodb
```

Further reading
===============

* MongoDB 3.2 WiredTiger Benchmark: https://dzone.com/articles/mongodb-32-wiredtiger-in-iibench
* MongoDB benchmark: sysbench-mongodb IO-bound workload comparison: https://www.percona.com/blog/2015/07/14/mongodb-benchmark-sysbench-mongodb-io-bound-workload-comparison/
* Linkbench for MySQL & MongoDB with a cached database: http://smalldatum.blogspot.com/2015/07/linkbench-for-mysql-mongodb-with-cached.html
