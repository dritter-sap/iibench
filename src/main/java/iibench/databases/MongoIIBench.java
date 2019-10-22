package iibench.databases;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import iibench.IIbenchConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class MongoIIBench implements DBIIBench {
  private static final Logger log = LoggerFactory.getLogger(MongoIIBench.class);

  private final Consumer<Document> emptyConsumer = document -> {};

  private IIbenchConfig config;
  private WriteConcern  myWC;
  private MongoDatabase db;
  private MongoClient   client;
  private String        indexTechnology;
  private MongoCollection<Document> coll;

  public MongoIIBench(final IIbenchConfig config) {
    this.config = config;
    myWC = WriteConcern.JOURNALED;
  }

  @Override
  public void connect() throws Exception {
    final MongoClientOptions clientOptions = new MongoClientOptions.Builder().connectionsPerHost(2048).socketTimeout(600000)
        .writeConcern(myWC).build();
    final ServerAddress srvrAdd = new ServerAddress(config.getServerName(), config.getServerPort());
    client = new MongoClient(srvrAdd, clientOptions);

    log.debug("mongoOptions | {}.", client.getMongoOptions().toString());
    log.debug("mongoWriteConcern | {}.", client.getWriteConcern().toString());
    db = client.getDatabase(config.getDbName());
  }

  @Override
  public void disconnect() {
    this.db.drop();
    this.client.close();
  }

  @Override
  public void checkIndexUsed() {
    final Document buildInfo = db.runCommand(new Document("buildInfo", 1));

    // check if tokumxVersion exists, otherwise assume mongo
    if (buildInfo.toString().contains("tokumxVersion")) {
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
    if (indexTechnology.toLowerCase().equals("mongo")) {
      coll = db.getCollection(name);
      return;
    } else {
      throw new IllegalStateException("Unknown Indexing Technology " + indexTechnology + ", shutting down");
    }
  }

  @Override
  public void createIndexForCollection() {
    final BasicDBObject idxOptions = new BasicDBObject();
    idxOptions.put("background", false);

    if (indexTechnology.toLowerCase().equals("tokumx")) {
      idxOptions.put("compression", config.getCompressionType());
      idxOptions.put("readPageSize", config.getBasementSize());
    }
    //coll.createIndex(Indexes.compoundIndex(Indexes.hashed("price"),
    //        Indexes.hashed("dateandtime"),
    //        Indexes.hashed("customerid"),
    //        Indexes.hashed("cashregisterid")));
    coll.createIndex(Indexes.hashed("price"));
    coll.createIndex(Indexes.hashed("dateandtime"));
    coll.createIndex(Indexes.hashed("customerid"));
    coll.createIndex(Indexes.hashed("cashregisterid"));
  }

  @Override
  public String getCollectionName() {
    return coll.getDocumentClass().getName(); //TODO: check .getName();
  }

  @Override
  public void insertDocumentToCollection(final List<Map<String, Object>> docs) {
    final List<Document> documents = new ArrayList<Document>();;
    for (final Map<String, Object> data : docs) {
      final Document doc = new Document();
      data.entrySet().stream().forEach(e -> doc.append(e.getKey(), e.getValue()));
      documents.add(doc);
    }
    coll.insertMany(documents);
  }

  @Override
  public long queryAndMeasureElapsed(final int whichQuery, final double thisPrice, final int thisCashRegisterId, final long thisRandomTime,
      final int thisCustomerId) {
    log.debug("Executed queryAndMeasureElapsed {}", whichQuery);
    long now = System.currentTimeMillis();
    if (whichQuery == 1) {
      coll.find(or(
              and(eq("price", thisPrice), eq("dateandtime", thisRandomTime), gte("customerid", thisCustomerId)),
              and(gt("price", thisPrice), eq("dateandtime", thisRandomTime)),
              and(gt("price", thisPrice))
      )).projection(fields(include("price", "dateandtime", "customerid")))
              .limit(config.getQueryLimit()).forEach(emptyConsumer);
    } else if (whichQuery == 2) {
      coll.find(or(
              and(eq("price", thisPrice), gte("customerid", thisCustomerId)),
              and(gt("price", thisPrice))
      )).projection(fields(include("price", "customerid")))
              .limit(config.getQueryLimit()).forEach(emptyConsumer);
    } else if (whichQuery == 3) {
      coll.find(or(
              and(eq("cashregisterid", thisCashRegisterId), eq("price", thisPrice), gte("customerid", thisCustomerId)),
              and(eq("cashregisterid", thisCashRegisterId), gt("price", thisPrice)),
              and(gt("cashregisterid", thisCashRegisterId))
      )).projection(fields(include("cashregisterid", "price", "customerid")))
              .limit(config.getQueryLimit()).forEach(emptyConsumer);
    }
    long elapsed = System.currentTimeMillis() - now;
    return elapsed;
  }
}
