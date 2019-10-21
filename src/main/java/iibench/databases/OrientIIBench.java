package iibench.databases;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import iibench.IIbenchConfig;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrientIIBench implements DBIIBench {
  private static final Logger log = LoggerFactory.getLogger(OrientIIBench.class);

  private IIbenchConfig config;
  private OrientDB      orient;
  private ODatabasePool pool; //pool not thread-safe
  private OClass        collection;

  public OrientIIBench(final IIbenchConfig config) {
    this.config = config;
  }

  @Override
  public void connect() throws Exception {
    final OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
    final OrientDBConfig oriendDBconfig = poolCfg.build();
    orient = new OrientDB("remote:localhost", "root", "root", oriendDBconfig);
    orient.create(config.getDbName(), ODatabaseType.PLOCAL);
    pool = new ODatabasePool(orient, this.config.getDbName(), "admin", "admin", oriendDBconfig);
  }

  @Override
  public void disconnect() {
    pool.close();
    orient.drop(config.getDbName());
    orient.close();
  }

  @Override
  public void checkIndexUsed() {
    log.debug("Index technology = {}", OClass.INDEX_TYPE.NOTUNIQUE);
  }

  @Override
  public void createCollection(final String name) {
    try (final ODatabaseSession session = pool.acquire()) {
      collection = session.createClass(name);
      collection.createProperty("cashregisterid", OType.INTEGER);
    }
  }

  @Override
  public void createIndexForCollection() {
    try (final ODatabaseSession session = pool.acquire()) {
      session.getClass(collection.getName())
          .createIndex(collection.getName() + "idx", OClass.INDEX_TYPE.NOTUNIQUE, "cashregisterid");
    }
  }

  @Override
  public String getCollectionName() {
    return collection.getName();
  }

  @Override
  public void insertDocumentToCollection(final List<Map<String, Object>> docs) {
    try (final ODatabaseSession session = pool.acquire()) {
      session.begin();
      for (final Map<String, Object> data : docs) {
        final OElement element = session.newInstance(collection.getName());
        data.entrySet().stream().forEach(e -> element.setProperty(e.getKey(), e.getValue()));
        element.save();
      }
      session.commit();
    }
  }

  @Override
  public long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime,
      int thisCustomerId) {

    try (final ODatabaseSession session = pool.acquire()) {
      final String query1a = "SELECT price, dateandtime, customerid FROM %s FORCE INDEX (pdc) " +
              "WHERE '(price=%.2f and dateandtime=\"%s\" and customerid>=%d) OR '\\\n" +
              "'(price=%.2f and dateandtime>\"%s\") OR '\\\n" +
              "'(price>%.2f) LIMIT %d'";
    }


    /*final BasicDBObject query = new BasicDBObject();
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

      keys.put("price", 1);
      keys.put("dateandtime", 1);
      keys.put("customerid", 1);
      keys.put("_id", 0);

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

      keys.put("price", 1);
      keys.put("customerid", 1);
      keys.put("_id", 0);

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

      keys.put("cashregisterid", 1);
      keys.put("price", 1);
      keys.put("customerid", 1);
      keys.put("_id", 0);
    }

    //TODO: logMe("Executed queryAndMeasureElapsed %d",whichQuery);
    long now = System.currentTimeMillis();
    DBCursor cursor = null;
    try {
      cursor = coll.find(query, keys).limit(config.getQueryLimit());
      while (cursor.hasNext()) {
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
    return elapsed; */
    return 0;
  }
}
