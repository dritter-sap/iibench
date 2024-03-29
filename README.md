iibench [![Build Status](https://travis-ci.com/dritter-sap/iibench.svg?branch=master)](https://travis-ci.com/dritter-sap/iibench)
===============

The **Indexed insertion Benchmark** (short iiBench) for document stores like [MongoDB](https://www.mongodb.com/) and [OrientDB](https://orientdb.org/) assesses the behavior of an index under permanent load (concurrent inserts) and queries on the same index at the same time.
The original benchmark was developed by [Percona](https://www.percona.com/) (cf. https://github.com/tmcallaghan/iibench-mongodb), specifically targetting MongoDB and last updated in 2014.

> This repository was transferred from [dritter-sap/iibench-mongodb](https://github.com/dritter-sap/iibench-mongodb), originally forked from [iibench-mongodb](https://github.com/tmcallaghan/iibench-mongodb), but not MongoDB-specific any longer.

> **This repository denotes the latest version of the benchmark. All further contributions should go here.**

**Contributions (so far):**
* fixed bugs (e.g., threading problems)
* ported benchmark to mongodb `v3.11.1`
* added a benchmark for orientdb `v3.2.0` (latest minor version)
* added post-processing for visualization of results, plotting (using [xchart](https://github.com/knowm/XChart), and [csv-jdbc](http://csvjdbc.sourceforge.net/doc.html))
* improved CLI (using [airline](https://github.com/airlift/airline))

**Supported indexes**

- hash
- range / tree

(see open topics / issues below)

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
git clone https://github.com/dritter-sap/iibench.git
cd iibench
mvn clean package

```

<!---*[optionally edit run.simple.bash to modify the benchmark behavior]*-->

*OrientDB*

> start via docker, e.g. `docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=root orientdb:latest`

```bash 
[cd target]

java -jar iibench.jar -host plocal:localhost -user root -password <password> -maxRows 1000000 -numDocsPerInsert 1000 -queryNumDocsBegin 1000000 -numWriterThreads 1 -numQueryThreads 1 -dbType orientdb

```

*MongoDB*

> start via docker, e.g. `docker run --net host --rm -d -p 27017:27017 mongo`

```bash
[cd target]

java -jar iibench.jar -host localhost -port 27017 -user root -password <password> -maxRows 1000000 -numDocsPerInsert 1000 -queryNumDocsBegin 100000 -numWriterThreads 1 -numQueryThreads 1 -dbType mongodb
```

**Open topics / issues**

- [ ] initializer: more sophisticated data model + queries, JSON-specific
- [ ] fix multi-threading
- [ ] add an optional padding field to increase data set size quickly
  - [ ] user defined size
  - [ ] user defined compressible amount
- [ ] additional query workload features
  - [ ] randomly select query type (based on indexes)
- [ ] compare other features in Launchpad version
- [ ] Update driver versions:
  - [x] OrientDB `3.1.x`
  - [ ] OrientDB `3.2.x` (current version)
- [ ] allow for interleaved insert, query
- [ ] beyond Java 1.8
- [ ] move plotting to https://github.com/manuelh9r/XTChart?

Further reading
===============

* MongoDB 3.2 WiredTiger Benchmark: https://dzone.com/articles/mongodb-32-wiredtiger-in-iibench
* MongoDB benchmark: sysbench-mongodb IO-bound workload comparison: https://www.percona.com/blog/2015/07/14/mongodb-benchmark-sysbench-mongodb-io-bound-workload-comparison/
* Linkbench for MySQL & MongoDB with a cached database: http://smalldatum.blogspot.com/2015/07/linkbench-for-mysql-mongodb-with-cached.html
* PostgreSQL vs. MongoDB: https://www.percona.com/live/e17/sites/default/files/slides/High%20Performance%20JSON%20-%20PostgreSQL%20vs.%20MongoDB%20-%20FileId%20-%20115573.pdf
* MongoDB sysbench: https://lab-docs.percona.com/en/latest/mongodb-sysbench-hppro2.html
