package iibench.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import iibench.IIbenchConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * CREATE COLLECTION purchases_index;
 * select * from m_collection_tables;
 * CREATE INDEX iibench_idx ON purchases_index (a,b);
 * DROP INDEX iibench_idx;
 * select count(*) from purchases_index;
 * select * from purchase_index;
 * drop collection purchases_index;
 */

public class HDBDocStoreIIBench implements DBIIBench{
    private static final Logger log = LoggerFactory.getLogger(HDBDocStoreIIBench.class);

    private static String connectionString = ""; //?autocommit=false
    private static String user             = "";
    private static String password         = "";

    private final IIbenchConfig config;
    private HikariDataSource ds;
    private String collectionName;

    public HDBDocStoreIIBench(IIbenchConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        final HikariConfig config = configureConnectionPool();
        ds = new HikariDataSource(config);

        try(final Connection connection = ds.getConnection()) { // DriverManager.getConnection(connectionString, user, password)
            if (connection == null) {
                throw new IllegalStateException("Could not connect to " + connectionString);
            }
        }
    }

    @Override
    public void disconnect() {
        try(final Connection connection = ds.getConnection(); final Statement stmt = connection.createStatement()) {
            stmt.execute("DROP COLLECTION " + collectionName + ";");
        } catch (final SQLException e) {
            log.error("Could not delete collection " + collectionName + ".", e);
        }
        ds.close();
    }

    @Override
    public void checkIndexUsed() {
        return;
    }

    @Override
    public void createCollection(final String name) {
        this.collectionName = name;
        try(final Connection connection = ds.getConnection(); final Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE COLLECTION " + name + ";");
        } catch (final SQLException e) {
            throw new IllegalStateException("Collection " + name + " could not be created", e);
        }
    }

    @Override
    public void createIndexForCollection() {
        try(final Connection connection = ds.getConnection(); final Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE HASH INDEX " + this.collectionName + "price" + " on " + this.collectionName + "(\"price\")" + ";");
            stmt.execute("CREATE HASH INDEX " + this.collectionName + "dateandtime" + " on " + this.collectionName + "(\"dateandtime\")" + ";");
            stmt.execute("CREATE HASH INDEX " + this.collectionName + "customerid" + " on " + this.collectionName + "(\"customerid\")" + ";");
            stmt.execute("CREATE HASH INDEX " + this.collectionName + "cashregisterid" + " on " + this.collectionName + "(\"cashregisterid\")" + ";");
        } catch (final SQLException e) {
            throw new IllegalStateException("Index for collection " + this.collectionName + " could not be created", e);
        }
        // log.error("No index created.");
    }

    @Override
    public String getCollectionName() {
        return this.collectionName;
    }

    @Override
    public void insertDocumentToCollection(final List<Map<String, Object>> docs) {
        insertPreparedBatch(docs);
    }

    private void insertBatch(List<Map<String, Object>> docs) {
        String sql = "INSERT INTO " + collectionName + " " +
                "VALUES('{\"a\":1, \"b\":2, \"c\":3, \"d\":4, \"e\":5}');";
        try(final Connection connection = ds.getConnection(); final Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            for (final Map<String, Object> data : docs) {
                final StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("INSERT INTO ").append(collectionName).append(" ").append("VALUES('{");
                data.entrySet().stream().forEach(e -> {
                    queryBuilder.append("\"").append(e.getKey()).append("\"")
                            .append(":");
                    if (e.getValue() instanceof String) {
                        queryBuilder.append("\"").append(e.getValue()).append("\"");
                    } else {
                        queryBuilder.append(e.getValue());
                    }
                    if (!e.getKey().equals("dateandtime")) {
                        queryBuilder.append(", ");
                    }
                });
                queryBuilder.append("}');");
                //stmt.addBatch(queryBuilder.toString());
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
            stmt.execute("COMMIT;");
        } catch (final SQLException e) {
            throw new IllegalStateException("Data could not be inserted into " + collectionName + ".", e);
        }
    }

    private void insertPreparedBatch(final List<Map<String, Object>> docs) {
        try(final Connection connection = ds.getConnection(); final PreparedStatement prepStmt = connection.prepareStatement("INSERT INTO " + collectionName + " VALUES(?)")) {
            connection.setAutoCommit(true);

            for (final Map<String, Object> doc : docs) {
                // data.entrySet().stream().forEach(e -> prepStmt.setInt(1, 1));
                final Iterator<Map.Entry<String, Object>> docIt = doc.entrySet().iterator();
                final Map.Entry<String, Object> first = docIt.next();
                final Map.Entry<String, Object> second = docIt.next();

                final StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("{\"" + first.getKey() + "\": " + first.getValue() + ", \"" + second.getKey() + "\": " + second.getValue())
                        .append("}");
                prepStmt.setString(1, queryBuilder.toString());
                prepStmt.addBatch();
            }
            prepStmt.executeBatch();
        } catch (final SQLException e) {
            throw new IllegalStateException("Data could not be inserted into " + collectionName + ".", e);
        }
    }

    @Override
    public long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime, int thisCustomerId) {
        return 0;
    }

    private HikariConfig configureConnectionPool() {
        final HikariConfig config = new HikariConfig();
        config.setMinimumIdle(3);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(3000);
        config.setIdleTimeout(TimeUnit.SECONDS.toMillis(10));
        config.setValidationTimeout(TimeUnit.SECONDS.toMillis(2));

        config.setJdbcUrl(connectionString);
        config.setUsername(user);
        config.setPassword(password);
        return config;
    }
}
