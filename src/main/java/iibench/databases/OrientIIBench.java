package iibench.databases;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import iibench.IIbenchConfig;

import java.util.List;
import java.util.Map;

public class OrientIIBench implements DBIIBench {
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
    OrientDBConfig config = poolCfg.build();
    orient = new OrientDB("remote:localhost", config);

    pool = new ODatabasePool(orient, "test", "admin", "admin", config);
  }

  @Override
  public void disconnect() {
    pool.close();
    orient.drop("test");
    orient.close();
  }

  @Override
  public void checkIndexUsed() {
    //TODO: logMe("  index technology = %s",indexTechnology);

    //if (indexTechnology.toLowerCase().equals("tokumx")) {
    //TODO: logMe("  + compression type = %s", config.getCompressionType());
    //TODO: logMe("  + basement node size (bytes) = %d", config.getBasementSize());
    //}
  }

  @Override
  public void createCollection(final String name) {
    // TODO: clustering?
    try (final ODatabaseSession session = pool.acquire()) {
      collection = session.createClass(name);
      collection.createProperty("cashregisterid", OType.INTEGER);
    }
  }

  @Override
  public void createIndexForCollection() {
    // TODO: which index, compression, other settings, fields indexed?
    try (final ODatabaseSession session = pool.acquire()) {
      session.getClass(collection.getName())
          .createIndex(collection.getName() + "idx", OClass.INDEX_TYPE.NOTUNIQUE, "cashregisterid");
    }
    // collection.createIndex(collection.getName()+"idx", OClass.INDEX_TYPE.NOTUNIQUE, "cashregisterid");
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
        //final ODocument doc = new ODocument(collection.getName());
        //data.entrySet().stream().forEach(e -> doc.field(e.getKey(), e.getValue()));
        //doc.save();
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
    return 0;
  }
}
