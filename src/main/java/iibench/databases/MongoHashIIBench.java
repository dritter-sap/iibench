package iibench.databases;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import iibench.DBIIBench;
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

public class MongoHashIIBench implements DBIIBench {
  private static final Logger log = LoggerFactory.getLogger(MongoHashIIBench.class);

  private final Consumer<Document> emptyConsumer = document -> {};

  private WriteConcern  myWC;
  private MongoDatabase db;
  private MongoClient   client;
  private String        indexTechnology;
  private MongoCollection<Document> coll;

  public MongoHashIIBench() {
    myWC = WriteConcern.JOURNALED;
  }

  @Override
  public void connect(final String serverName, final Integer serverPort, final String dbName, final String userName, final String password) throws Exception {
    final MongoClientOptions clientOptions = new MongoClientOptions.Builder().connectionsPerHost(2048).socketTimeout(600000)
        .writeConcern(myWC).build();
    final ServerAddress srvrAdd = new ServerAddress(serverName, serverPort);
    client = new MongoClient(srvrAdd, clientOptions);

    log.debug("mongoOptions | {}.", client.getMongoOptions().toString());
    log.debug("mongoWriteConcern | {}.", client.getWriteConcern().toString());
    db = client.getDatabase(dbName);
  }

  @Override
  public void disconnect(final String dbName) {
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
      log.debug("  + compression type = {}", "zlib");
      log.debug("  + basement node size (bytes) = {}", 65536);
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
  public void insertDocumentToCollection(final List<Map<String, Object>> docs, final int numDocumentsPerInsert) {
    final List<Document> documents = new ArrayList<Document>();
    for (final Map<String, Object> data : docs) {
      final Document doc = new Document();
      data.entrySet().stream().forEach(e -> doc.append(e.getKey(), e.getValue()));
      documents.add(doc);
    }
    coll.insertMany(documents);
  }

  @Override
  public long queryAndMeasureElapsed(final int whichQuery, final double thisPrice, final int thisCashRegisterId, final long thisRandomTime,
      final int thisCustomerId, final int queryLimit) {
    log.debug("Executed queryAndMeasureElapsed {}", whichQuery);
    long now = System.currentTimeMillis();
    if (whichQuery == 1) {
      coll.find(or(
              and(eq("price", thisPrice), eq("dateandtime", thisRandomTime), gte("customerid", thisCustomerId)),
              and(gt("price", thisPrice), eq("dateandtime", thisRandomTime)),
              and(gt("price", thisPrice))
      )).projection(fields(include("price", "dateandtime", "customerid")))
              .limit(queryLimit).forEach(emptyConsumer);
    } else if (whichQuery == 2) {
      coll.find(or(
              and(eq("price", thisPrice), gte("customerid", thisCustomerId)),
              and(gt("price", thisPrice))
      )).projection(fields(include("price", "customerid")))
              .limit(queryLimit).forEach(emptyConsumer);
    } else if (whichQuery == 3) {
      coll.find(or(
              and(eq("cashregisterid", thisCashRegisterId), eq("price", thisPrice), gte("customerid", thisCustomerId)),
              and(eq("cashregisterid", thisCashRegisterId), gt("price", thisPrice)),
              and(gt("cashregisterid", thisCashRegisterId))
      )).projection(fields(include("cashregisterid", "price", "customerid")))
              .limit(queryLimit).forEach(emptyConsumer);
    }
    long elapsed = System.currentTimeMillis() - now;
    return elapsed;
  }
}
