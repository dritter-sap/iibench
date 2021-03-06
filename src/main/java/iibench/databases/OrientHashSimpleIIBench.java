package iibench.databases;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import iibench.DBIIBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrientHashSimpleIIBench implements DBIIBench {
  private static final Logger log = LoggerFactory.getLogger(OrientHashSimpleIIBench.class);

  private static final String query1 = "SELECT price, dateandtime, customerid " + "FROM `purchases_index` "
      + "WHERE dateandtime <= :dateandtime";

  private static final String query2 =
      "SELECT price, customerid " + "FROM `purchases_index` "
              + "WHERE <= :customerid";

  private static final String query3 = "SELECT price, dateandtime, customerid " + "FROM `purchases_index` "
      + "WHERE cashregisterid <= :cashregisterid";

  private OrientDB      orient;
  private ODatabasePool pool; //pool not thread-safe
  private OClass        collection;

  public OrientHashSimpleIIBench() {
  }

  @Override
  public void connect(final String serverName, final Integer serverPort, final String dbName, final String userName,
      final String password) throws Exception {
    final OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
    final OrientDBConfig oriendDBconfig = poolCfg.build();
    orient = new OrientDB("remote:localhost", userName, password, oriendDBconfig);
    orient.create(dbName, ODatabaseType.PLOCAL);
    pool = new ODatabasePool(orient, dbName, "admin", "admin", oriendDBconfig);
  }

  @Override
  public void disconnect(final String dbName) {
    pool.close();
    orient.drop(dbName);
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
      collection.createProperty("dateandtime", OType.LONG);
      collection.createProperty("customerid", OType.INTEGER);
      collection.createProperty("productid", OType.INTEGER);
      collection.createProperty("price", OType.DOUBLE);
    }
  }

  @Override
  public void createIndexForCollection() {
    try (final ODatabaseSession session = pool.acquire()) {
      final OClass cls = session.getClass(collection.getName());
      cls.createIndex(collection.getName() + "idxp", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "price");
      cls.createIndex(collection.getName() + "idxd", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "dateandtime");
      cls.createIndex(collection.getName() + "idxc", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "customerid");
      cls.createIndex(collection.getName() + "idxch", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "cashregisterid");
    }
  }

  @Override
  public String getCollectionName() {
    return collection.getName();
  }

  @Override
  public void insertDocumentToCollection(final List<Map<String, Object>> docs, final int numDocumentsPerInsert) {
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
      int thisCustomerId, final int queryLimit) {
    final Map<String, Object> params = new HashMap<>();
    params.put("price", thisPrice);
    params.put("dateandtime", thisRandomTime);
    params.put("customerid", thisCustomerId);
    params.put("cashregisterid", thisCashRegisterId);
    params.put("limit", queryLimit);

    if (whichQuery == 1) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query1, params)) {
        rs.stream().forEach(e -> {
        });
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else if (whichQuery == 2) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query2, params)) {
        rs.stream().forEach(e -> {
        });
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else if (whichQuery == 3) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query3, params)) {
        rs.stream().forEach(e -> {
        });
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else {
      throw new IllegalArgumentException("Query " + whichQuery + " unknown. Provide a query from {1,..,3}");
    }
  }
}
