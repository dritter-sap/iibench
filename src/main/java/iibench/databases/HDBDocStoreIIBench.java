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

public class HDBDocStoreIIBench implements DBIIBench {
    private static final Logger log = LoggerFactory.getLogger(HDBDocStoreIIBench.class);

    private HikariDataSource ds;
    private String collectionName;

    public HDBDocStoreIIBench() {
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
            if (whichQuery == 1) {
                try(final Statement stmt = connection.createStatement()) {
                    final String query1 = "SELECT \"price\", \"dateandtime\", \"customerid\" " +
                            "FROM purchases_index " +
                            "WHERE \"price\" = " + thisPrice + " and \"dateandtime\" = " + thisRandomTime + " and \"customerid\" >= " + thisCustomerId + " " +
                            "OR \"price\" > " + thisPrice + " and \"dateandtime\" = " + thisRandomTime + " " +
                            "OR \"price\" > " + thisPrice + " " +
                            "LIMIT " + queryLimit + ";";
                    long now = System.currentTimeMillis();
                    final ResultSet rs = stmt.executeQuery(query1);
                    while (rs.next()) {

                    }
                    long elapsed = System.currentTimeMillis() - now;
                    return elapsed;
                }
            } else if (whichQuery == 2) {
                try(final Statement stmt = connection.createStatement()) {
                    final String query2 = "SELECT \"price\", \"customerid\" " +
                            "FROM purchases_index " +
                            "WHERE \"price\" = " + thisPrice + " and \"customerid\" >= " + thisCustomerId + " " +
                            "OR \"price\" > " + thisPrice + " " +
                            "LIMIT " + queryLimit + ";";
                    long now = System.currentTimeMillis();
                    final ResultSet rs = stmt.executeQuery(query2);
                    while (rs.next()) {

                    }
                    long elapsed = System.currentTimeMillis() - now;
                    return elapsed;
                }
            } else if (whichQuery == 3) {
                try(final Statement stmt = connection.createStatement()) {
                    final String sql = "SELECT \"price\", \"dateandtime\", \"customerid\" " +
                            "FROM purchases_index " +
                            "WHERE \"cashregisterid\" = " + thisCashRegisterId + " and \"price\" = " + thisPrice + " and \"customerid\" >= " + thisCustomerId + " " +
                            "OR \"cashregisterid\" = " + thisCashRegisterId + " and \"price\" > " + thisPrice + " " +
                            "OR \"cashregisterid\" > " + thisCashRegisterId + " " +
                            "LIMIT " + queryLimit + ";";
                    long now = System.currentTimeMillis();
                    final ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {

                    }
                    long elapsed = System.currentTimeMillis() - now;
                    return elapsed;
                }
            } else {
                throw new IllegalArgumentException("Query " + whichQuery + " unknown. Provide a query from {1,..,3}");
            }
        } catch (final SQLException e) {
            throw new IllegalStateException("Data could not be queried from " + collectionName + ".", e);
        }
    }

    @Deprecated
    private long query1UnsupportedPreparedStatement(double thisPrice, long thisRandomTime, int thisCustomerId, Connection connection, final int queryLimit) throws SQLException {
        try(final PreparedStatement prepStmt = connection.prepareStatement("SELECT \"price\", \"dateandtime\", \"customerid\" " +
                "FROM purchases_index " +
                "WHERE \"price\" = ? and \"dateandtime\" = ? and \"customerid\" >= ? " +
                "OR \"price\" > ? and \"dateandtime\" = ? " +
                "OR \"price\" > ? " +
                "LIMIT ?;"
        )) {
            prepStmt.setDouble(1, thisPrice);
            prepStmt.setLong(2, thisRandomTime);
            prepStmt.setInt(3, thisCustomerId);
            prepStmt.setDouble(4, thisPrice);
            prepStmt.setLong(5, thisRandomTime);
            prepStmt.setDouble(6, thisPrice);
            prepStmt.setDouble(7, queryLimit);

            long now = System.currentTimeMillis();
            final ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {

            }
            long elapsed = System.currentTimeMillis() - now;
            return elapsed;
        }
    }

    @Deprecated
    private long query2UnsupportedPreparedStatement(double thisPrice, int thisCustomerId, Connection connection, final int queryLimit) throws SQLException {
        try(final PreparedStatement prepStmt = connection.prepareStatement("SELECT \"price\", \"customerid\" " +
                "FROM purchases_index " +
                "WHERE \"price\" = ? and \"customerid\" >= ? " +
                "OR \"price\" > ? " +
                "LIMIT ?")) {
            prepStmt.setDouble(1, thisPrice);
            prepStmt.setInt(2, thisCustomerId);
            prepStmt.setDouble(3, thisPrice);
            prepStmt.setDouble(4, queryLimit);

            long now = System.currentTimeMillis();
            final ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {

            }
            long elapsed = System.currentTimeMillis() - now;
            return elapsed;
        }
    }

    @Deprecated
    private long query3UnsupportedPreparedStatement(double thisPrice, int thisCashRegisterId, int thisCustomerId, Connection connection, final int queryLimit) throws SQLException {
        try(final PreparedStatement prepStmt = connection.prepareStatement("SELECT \"price\", \"dateandtime\", \"customerid\" " +
                "FROM purchases_index " +
                "WHERE \"cashregisterid\" = ? and \"price\" = ? and \"customerid\" >= ? " +
                "OR \"cashregisterid\" = ? and \"price\" > ? " +
                "OR \"cashregisterid\" > ? " +
                "LIMIT ?;")) {
            prepStmt.setInt(1, thisCashRegisterId);
            prepStmt.setDouble(2, thisPrice);
            prepStmt.setInt(3, thisCustomerId);
            prepStmt.setInt(4, thisCashRegisterId);
            prepStmt.setDouble(5, thisPrice);
            prepStmt.setInt(6, thisCashRegisterId);
            prepStmt.setDouble(7, queryLimit);

            long now = System.currentTimeMillis();
            final ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {

            }
            long elapsed = System.currentTimeMillis() - now;
            return elapsed;
        }
    }

    private HikariConfig configureConnectionPool(final String host, final Integer port, final String userNAme, final String password) {
        final HikariConfig poolConfig = new HikariConfig();
        poolConfig.setMinimumIdle(3);
        poolConfig.setMaximumPoolSize(10);
        poolConfig.setConnectionTimeout(3000);
        poolConfig.setIdleTimeout(TimeUnit.SECONDS.toMillis(10));
        poolConfig.setValidationTimeout(TimeUnit.SECONDS.toMillis(2));

        poolConfig.setJdbcUrl("jdbc:sap://" + host + ":" + port + "/");
        poolConfig.setUsername(userNAme);
        poolConfig.setPassword(password);
        return poolConfig;
    }
}
