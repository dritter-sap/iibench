package iibench.databases;

import com.mongodb.*;
import iibench.IIbenchConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoAccessor {
    private IIbenchConfig config;
    private WriteConcern myWC;
    private DB db;
    private MongoClient client;
    private String indexTechnology;
    private DBCollection coll;

    public MongoAccessor(final IIbenchConfig config) {
        this.config = config;

        myWC = new WriteConcern();
        if (config.getWriteConcern().toLowerCase().equals("fsync_safe")) {
            myWC = WriteConcern.FSYNC_SAFE;
        }
        else if ((config.getWriteConcern().toLowerCase().equals("none"))) {
            myWC = WriteConcern.NONE;
        }
        else if ((config.getWriteConcern().toLowerCase().equals("normal"))) {
            myWC = WriteConcern.NORMAL;
        }
        else if ((config.getWriteConcern().toLowerCase().equals("replicas_safe"))) {
            myWC = WriteConcern.REPLICAS_SAFE;
        }
        else if ((config.getWriteConcern().toLowerCase().equals("safe"))) {
            myWC = WriteConcern.SAFE;
        }
        else {
            throw new UnsupportedOperationException("Write concern " + config.getWriteConcern() + " is not supported");
        }
    }

    public void connect() throws Exception {
        final MongoClientOptions clientOptions = new MongoClientOptions.Builder().connectionsPerHost(2048).socketTimeout(600000).writeConcern(myWC).build();
        final ServerAddress srvrAdd = new ServerAddress(config.getServerName(), config.getServerPort());
        client = new MongoClient(srvrAdd, clientOptions);

        //TODO: logMe("mongoOptions | " + m.getMongoOptions().toString());
        //TODO: logMe("mongoWriteConcern | " + m.getWriteConcern().toString());
        db = client.getDB(config.getDbName());
    }

    public void disconnect() {
        this.client.close();
    }

    public void checkIndexUsed() {
        // determine server type : mongo or tokumx
        // DBObject checkServerCmd = new BasicDBObject();
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

        //TODO: logMe("  index technology = %s",indexTechnology);

        if (indexTechnology.toLowerCase().equals("tokumx")) {
            //TODO: logMe("  + compression type = %s", config.getCompressionType());
            //TODO: logMe("  + basement node size (bytes) = %d", config.getBasementSize());
        }
    }

    public void createCollection(final String name) {
        if (indexTechnology.toLowerCase().equals("tokumx")) {
            DBObject cmd = new BasicDBObject();
            cmd.put("create", name);
            cmd.put("compression", config.getCompressionType());
            cmd.put("readPageSize", config.getBasementSize());
            CommandResult result = db.command(cmd);
            //logMe(result.toString());
        } else if (indexTechnology.toLowerCase().equals("mongo")) {
            // nothing special to do for a regular mongo collection
        } else {
            throw new IllegalStateException("Unknown Indexing Technology " + indexTechnology + ", shutting down");
        }
        coll = db.getCollection(name);
    }

    public void createIndexForCollection() {
        final BasicDBObject idxOptions = new BasicDBObject();
        idxOptions.put("background",false);

        if (indexTechnology.toLowerCase().equals("tokumx")) {
            idxOptions.put("compression",config.getCompressionType());
            idxOptions.put("readPageSize",config.getBasementSize());
        }

        if (config.getNumSecondaryIndexes() >= 1) {
            //TODO: logMe(" *** creating secondary index on price + customerid");
            coll.ensureIndex(new BasicDBObject("price", 1).append("customerid", 1), idxOptions);
        }
        if (config.getNumSecondaryIndexes() >= 2) {
            //TODO: logMe(" *** creating secondary index on cashregisterid + price + customerid");
            coll.ensureIndex(new BasicDBObject("cashregisterid", 1).append("price", 1).append("customerid", 1), idxOptions);
        }
        if (config.getNumSecondaryIndexes() >= 3) {
            //TODO: logMe(" *** creating secondary index on price + dateandtime + customerid");
            coll.ensureIndex(new BasicDBObject("price", 1).append("dateandtime", 1).append("customerid", 1), idxOptions);
        }
    }

    public String getCollectionName() {
        return coll.getName();
    }

    public void insertDocumentToCollection(final List<Map<String, Object>> docs) {
        final BasicDBObject[] aDocs = new BasicDBObject[config.getNumDocumentsPerInsert()];
        int i = 0;
        for (final Map<String, Object> data : docs) {
            final BasicDBObject doc = new BasicDBObject();
            data.entrySet().stream().forEach(e -> doc.put(e.getKey(), e.getValue()));
            aDocs[i]=doc;
            i++;
        }

        coll.insert(aDocs);
    }

    public long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime, int thisCustomerId) {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject keys = new BasicDBObject();

        if (whichQuery == 1) {
            BasicDBObject query1a = new BasicDBObject();
            query1a.put("price", thisPrice);
            query1a.put("dateandtime", thisRandomTime);
            query1a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            BasicDBObject query1b = new BasicDBObject();
            query1b.put("price", thisPrice);
            query1b.put("dateandtime", new BasicDBObject("$gt", thisRandomTime));

            BasicDBObject query1c = new BasicDBObject();
            query1c.put("price", new BasicDBObject("$gt", thisPrice));

            ArrayList<BasicDBObject> list1 = new ArrayList<BasicDBObject>();
            list1.add(query1a);
            list1.add(query1b);
            list1.add(query1c);

            query.put("$or", list1);

            keys.put("price",1);
            keys.put("dateandtime",1);
            keys.put("customerid",1);
            keys.put("_id",0);

        } else if (whichQuery == 2) {
            BasicDBObject query2a = new BasicDBObject();
            query2a.put("price", thisPrice);
            query2a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            BasicDBObject query2b = new BasicDBObject();
            query2b.put("price", new BasicDBObject("$gt", thisPrice));

            ArrayList<BasicDBObject> list2 = new ArrayList<BasicDBObject>();
            list2.add(query2a);
            list2.add(query2b);

            query.put("$or", list2);

            keys.put("price",1);
            keys.put("customerid",1);
            keys.put("_id",0);

        } else if (whichQuery == 3) {
            BasicDBObject query3a = new BasicDBObject();
            query3a.put("cashregisterid", thisCashRegisterId);
            query3a.put("price", thisPrice);
            query3a.put("customerid", new BasicDBObject("$gte", thisCustomerId));

            BasicDBObject query3b = new BasicDBObject();
            query3b.put("cashregisterid", thisCashRegisterId);
            query3b.put("price", new BasicDBObject("$gt", thisPrice));

            BasicDBObject query3c = new BasicDBObject();
            query3c.put("cashregisterid", new BasicDBObject("$gt", thisCashRegisterId));

            ArrayList<BasicDBObject> list3 = new ArrayList<BasicDBObject>();
            list3.add(query3a);
            list3.add(query3b);
            list3.add(query3c);

            query.put("$or", list3);

            keys.put("cashregisterid",1);
            keys.put("price",1);
            keys.put("customerid",1);
            keys.put("_id",0);
        }

        //logMe("Executed queryAndMeasureElapsed %d",whichQuery);
        long now = System.currentTimeMillis();
        DBCursor cursor = null;
        try {
            cursor = coll.find(query,keys).limit(config.getQueryLimit());
            while(cursor.hasNext()) {
                //System.out.println(cursor.next());
                cursor.next();
            }
            cursor.close();
            cursor = null;
        } catch (Exception e) {
            //TODO: logMe("Query thread %d : EXCEPTION",threadNumber);
            e.printStackTrace();
            if (cursor != null)
                cursor.close();
        }
        long elapsed = System.currentTimeMillis() - now;
        return elapsed;
    }
}
