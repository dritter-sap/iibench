package iibench.databases;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import iibench.IIbenchConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrientIIBench implements DBIIBench {
  private static final Logger log = LoggerFactory.getLogger(OrientIIBench.class);

  private static final String query1 = "SELECT price, dateandtime, customerid " +
          "FROM `purchases_index` " +
          "WHERE price = :price and dateandtime = :dateandtime and customerid >= :customerid " +
          "OR price > :price and dateandtime = :dateandtime " +
          "OR price > :price " +
          "LIMIT :limit";

  private static final String query2 = "SELECT price, customerid " +
          "FROM `purchases_index` " +
          "WHERE price = :price and customerid >= :customerid " +
          "OR price > :price " +
          "LIMIT :limit";

  private static final String query3 = "SELECT price, dateandtime, customerid " +
          "FROM `purchases_index` " +
          "WHERE cashregisterid = :cashregisterid and price = :price and customerid >= :customerid " +
          "OR cashregisterid = :cashregisterid and price > :price  " +
          "OR cashregisterid > :cashregisterid " +
          "LIMIT :limit";

  private IIbenchConfig config;
  private OrientDB      orient;
  private ODatabasePool pool; //pool not thread-safe
  private OClass        collection;

  public OrientIIBench(final IIbenchConfig config) {
    this.config = config;
  }

  @Override
  public void connect(final String userName, final String password) throws Exception {
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
      collection.createProperty("price", OType.DOUBLE);
      collection.createProperty("dateandtime", OType.LONG);
      collection.createProperty("customerid", OType.INTEGER);
      collection.createProperty("cashregisterid", OType.INTEGER);
    }
  }

  @Override
  public void createIndexForCollection() {
    try (final ODatabaseSession session = pool.acquire()) {
      session.getClass(collection.getName())
          .createIndex(collection.getName() + "idx", OClass.INDEX_TYPE.NOTUNIQUE, "price","dateandtime","customerid","cashregisterid");
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
    final Map<String, Object> params = new HashMap<>();
    params.put("price", thisPrice);
    params.put("dateandtime", thisRandomTime);
    params.put("customerid", thisCustomerId);
    params.put("cashregisterid", thisCashRegisterId);
    params.put("limit", config.getQueryLimit());

    if (whichQuery == 1) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query1, params)) {
        rs.stream().forEach(e -> {});
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else if (whichQuery == 2) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query2, params)) {
        rs.stream().forEach(e -> {});
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else if (whichQuery == 3) {
      long now = System.currentTimeMillis();
      try (final ODatabaseSession session = pool.acquire(); final OResultSet rs = session.query(query3, params)) {
        rs.stream().forEach(e -> {});
      }
      long elapsed = System.currentTimeMillis() - now;
      return elapsed;
    } else {
      throw new IllegalArgumentException("Query " + whichQuery + " unknown. Provide a query from {1,..,3}");
    }
  }
}
