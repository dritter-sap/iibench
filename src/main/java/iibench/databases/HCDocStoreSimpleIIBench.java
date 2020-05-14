package iibench.databases;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import iibench.DBIIBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HCDocStoreSimpleIIBench implements DBIIBench {
    private static final Logger log = LoggerFactory.getLogger(HCDocStoreSimpleIIBench.class);

    private HikariDataSource ds;
    private String collectionName;

    public HCDocStoreSimpleIIBench() {
    }

    @Override
    public void connect(final String serverName, final Integer serverPort, final String dbName, final String userName, final String password) throws Exception {
        final HikariConfig poolConfig = configureConnectionPool(serverName, serverPort, userName, password);
        ds = new HikariDataSource(poolConfig);

        try(final Connection connection = ds.getConnection()) { // DriverManager.getConnection(connectionString, user, password)
            if (connection == null) {
                throw new IllegalStateException("Could not connect to " + serverName
                + ":" + serverPort);
            }
        }
    }

    @Override
    public void disconnect(final String dbName) {
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
            stmt.execute("CREATE HASH INDEX " + this.collectionName + "customerid" + " on " + this.collectionName + "(\"customerid\")" + ";");
        } catch (final SQLException e) {
            throw new IllegalStateException("Index for collection " + this.collectionName + " could not be created", e);
        }
    }

    @Override
    public String getCollectionName() {
        return this.collectionName;
    }

    @Override
    public void insertDocumentToCollection(final List<Map<String, Object>> docs, final int numDocumentsPerInsert) {
        insertPreparedBatch(docs);
    }

    private void insertPreparedBatch(final List<Map<String, Object>> docs) {
        try(final Connection connection = ds.getConnection(); final PreparedStatement prepStmt = connection.prepareStatement("INSERT INTO " + collectionName + " VALUES(?)")) {
            connection.setAutoCommit(true);

            for (final Map<String, Object> doc : docs) {
                final StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("{");
                doc.entrySet().stream().forEach(e -> {
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
                queryBuilder.append("}");
                prepStmt.setString(1, queryBuilder.toString());
                prepStmt.addBatch();
            }
            prepStmt.executeBatch();
        } catch (final SQLException e) {
            throw new IllegalStateException("Data could not be inserted into " + collectionName + ".", e);
        }
    }

    @Override
    public long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime,
                                       int thisCustomerId, final int queryLimit) {
        try(final Connection connection = ds.getConnection()) {
            /*try(final PreparedStatement prepStmt = connection.prepareStatement("SELECT \"price\", \"dateandtime\", \"customerid\" " +
                    "FROM purchases_index " +
                    "WHERE \"customerid\" <= 1000;"
            )) {
                long now = System.currentTimeMillis();
                final ResultSet rs = prepStmt.executeQuery();
                while (rs.next()) {

                }
                long elapsed = System.currentTimeMillis() - now;
                return elapsed;
            }*/
            try(final Statement stmt = connection.createStatement()) {
                final String query2 = "SELECT \"price\", \"customerid\" " +
                        "FROM purchases_index " +
                        "WHERE \"customerid\" = " + 1 + " " + ";";
                // System.out.println("Query:" + query2);
                long now = System.currentTimeMillis();
                final ResultSet rs = stmt.executeQuery(query2);
                while (rs.next()) {

                }
                long elapsed = System.currentTimeMillis() - now;
                return elapsed;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException("Data could not be queried from " + collectionName + ".", e);
        }
    }

    private HikariConfig configureConnectionPool(final String host, final Integer port, final String userNAme, final String password) {
        final HikariConfig poolConfig = new HikariConfig();
        poolConfig.setMinimumIdle(3);
        poolConfig.setMaximumPoolSize(10);
        poolConfig.setConnectionTimeout(3000);
        poolConfig.setIdleTimeout(TimeUnit.SECONDS.toMillis(10));
        poolConfig.setValidationTimeout(TimeUnit.SECONDS.toMillis(2));

        poolConfig.setJdbcUrl("jdbc:sap://" + host + ":" + port + "/" + "?encrypt=true");
        poolConfig.setUsername(userNAme);
        poolConfig.setPassword(password);
        return poolConfig;
    }
}
