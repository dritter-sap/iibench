package iibench.databases;

import com.mongodb.*;
import iibench.IIbenchConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoIIBenchOldAPI implements DBIIBench {
    private static final Logger log = LoggerFactory.getLogger(MongoIIBench.class);

    private IIbenchConfig config;
    private WriteConcern  myWC;
    private DB            db;
    private MongoClient   client;
    private String        indexTechnology;
    private DBCollection  coll;

    public MongoIIBenchOldAPI(final IIbenchConfig config) {
        this.config = config;

        myWC = WriteConcern.JOURNALED;
//    if (config.getWriteConcern().toLowerCase().equals("fsync_safe")) {
//      myWC = WriteConcern.FSYNC_SAFE;
//    } else if ((config.getWriteConcern().toLowerCase().equals("none"))) {
//      myWC = WriteConcern.SAFE;
//    } else if ((config.getWriteConcern().toLowerCase().equals("normal"))) {
//      myWC = WriteConcern.NORMAL;
//    } else if ((config.getWriteConcern().toLowerCase().equals("replicas_safe"))) {
//      myWC = WriteConcern.REPLICAS_SAFE;
//    } else if ((config.getWriteConcern().toLowerCase().equals("safe"))) {
//      myWC = WriteConcern.SAFE;
//    } else {
//      throw new UnsupportedOperationException("Write concern " + config.getWriteConcern() + " is not supported");
//    }
    }

    @Override
    public void connect(final String userName, final String password) throws Exception {
        final MongoClientOptions clientOptions = new MongoClientOptions.Builder().connectionsPerHost(2048).socketTimeout(600000)
                .writeConcern(myWC).build();
        final ServerAddress srvrAdd = new ServerAddress(config.getServerName(), config.getServerPort());
        client = new MongoClient(srvrAdd, clientOptions);

        log.debug("mongoOptions | {}.", client.getMongoOptions().toString());
        log.debug("mongoWriteConcern | {}.", client.getWriteConcern().toString());
        db = client.getDB(config.getDbName());
    }

    @Override
    public void disconnect() {
        this.db.dropDatabase();
        this.client.close();
    }

    @Override
    public void checkIndexUsed() {
        CommandResult commandResult = db.command("buildInfo");

        // check if tokumxVersion exists, otherwise assume mongo
        if (commandResult.toString().contains("tokumxVersion")) {
            indexTechnology = "tokumx";
        } else {
            indexTechnology = "mongo";
        }

        if ((!indexTechnology.toLowerCase().equals("tokumx")) && (!indexTechnology.toLowerCase().equals("mongo"))) {
            throw new IllegalStateException("Unknown Indexing Technology " + indexTechnology + ", shutting down");
        }

        log.debug("  index technology = {}",indexTechnology);

        if (indexTechnology.toLowerCase().equals("tokumx")) {

            log.debug("  + compression type = {}", config.getCompressionType());
            log.debug("  + basement node size (bytes) = {}", config.getBasementSize());
        }
    }

    @Override
    public void createCollection(final String name) {
        if (indexTechnology.toLowerCase().equals("tokumx")) {
            final DBObject cmd = new BasicDBObject();
            cmd.put("create", name);
            cmd.put("compression", config.getCompressionType());
            cmd.put("readPageSize", config.getBasementSize());
            final CommandResult result = db.command(cmd);
            log.debug("created collection: {}", result.toString());
        } else if (indexTechnology.toLowerCase().equals("mongo")) {
            // nothing special to do for a regular mongo collection
        } else {
            throw new IllegalStateException("Unknown Indexing Technology " + indexTechnology + ", shutting down");
        }
        coll = db.getCollection(name);
    }

    @Override
    public void createIndexForCollection() {
        final BasicDBObject idxOptions = new BasicDBObject();
        idxOptions.put("background", false);

        if (indexTechnology.toLowerCase().equals("tokumx")) {
            idxOptions.put("compression", config.getCompressionType());
            idxOptions.put("readPageSize", config.getBasementSize());
        }
        final Map<String, Object> keys = new HashMap<>();
        keys.put("price", 1);
        keys.put("dateandtime", 1);
        keys.put("customerid", 1);
        keys.put("cashregisterid", 1);
        final BasicDBObject indexedFields = new BasicDBObject(keys);
        coll.createIndex(indexedFields, idxOptions);
    }

    @Override
    public String getCollectionName() {
        return coll.getName();
    }

    @Override
    public void insertDocumentToCollection(final List<Map<String, Object>> docs) {
        final BasicDBObject[] aDocs = new BasicDBObject[config.getNumDocumentsPerInsert()];
        int i = 0;
        for (final Map<String, Object> data : docs) {
            final BasicDBObject doc = new BasicDBObject();
            data.entrySet().stream().forEach(e -> doc.put(e.getKey(), e.getValue()));
            aDocs[i] = doc;
            i++;
        }
        coll.insert(aDocs);
    }

    @Override
    public long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime,
                                       int thisCustomerId) {
        final BasicDBObject query = new BasicDBObject();
        final BasicDBObject keys = new BasicDBObject();

        if (whichQuery == 1) {
            final BasicDBObject query1a = new BasicDBObject();
            query1a.put("price", thisPrice);
            query1a.put("dateandtime", thisRandomTime);
            query1a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            final BasicDBObject query1b = new BasicDBObject();
            query1b.put("price", thisPrice);
            query1b.put("dateandtime", new BasicDBObject("$gt", thisRandomTime));

            final BasicDBObject query1c = new BasicDBObject();
            query1c.put("price", new BasicDBObject("$gt", thisPrice));

            final List<BasicDBObject> list1 = new ArrayList<BasicDBObject>();
            list1.add(query1a);
            list1.add(query1b);
            list1.add(query1c);

            query.put("$or", list1);

            keys.put("price", 1);
            keys.put("dateandtime", 1);
            keys.put("customerid", 1);
            keys.put("_id", 0);

        } else if (whichQuery == 2) {
            final BasicDBObject query2a = new BasicDBObject();
            query2a.put("price", thisPrice);
            query2a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            final BasicDBObject query2b = new BasicDBObject();
            query2b.put("price", new BasicDBObject("$gt", thisPrice));

            final List<BasicDBObject> list2 = new ArrayList<BasicDBObject>();
            list2.add(query2a);
            list2.add(query2b);

            query.put("$or", list2);

            keys.put("price", 1);
            keys.put("customerid", 1);
            keys.put("_id", 0);

        } else if (whichQuery == 3) {
            final BasicDBObject query3a = new BasicDBObject();
            query3a.put("cashregisterid", thisCashRegisterId);
            query3a.put("price", thisPrice);
            query3a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            final BasicDBObject query3b = new BasicDBObject();
            query3b.put("cashregisterid", thisCashRegisterId);
            query3b.put("price", new BasicDBObject("$gt", thisPrice));

            final BasicDBObject query3c = new BasicDBObject();
            query3c.put("cashregisterid", new BasicDBObject("$gt", thisCashRegisterId));

            final List<BasicDBObject> list3 = new ArrayList<BasicDBObject>();
            list3.add(query3a);
            list3.add(query3b);
            list3.add(query3c);

            query.put("$or", list3);

            keys.put("cashregisterid", 1);
            keys.put("price", 1);
            keys.put("customerid", 1);
            keys.put("_id", 0);
        }

        log.debug("Executed queryAndMeasureElapsed {}", whichQuery);
        long now = System.currentTimeMillis();
        try(final DBCursor cursor = coll.find(query, keys).limit(config.getQueryLimit())) {
            while (cursor.hasNext()) {
                //System.out.println(cursor.next());
                cursor.next();
            }
        } catch (final Exception e) {
            // log.error("Query thread {} : EXCEPTION", threadNumber);
            e.printStackTrace();
        }
        long elapsed = System.currentTimeMillis() - now;
        return elapsed;
    }
}