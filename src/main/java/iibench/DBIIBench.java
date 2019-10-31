package iibench;

import java.util.List;
import java.util.Map;

public interface DBIIBench {
    void connect(String serverName, Integer serverPort, String dbName, String userName,
                 String password) throws Exception;

    void disconnect(String dbName);

    void checkIndexUsed();

    void createCollection(String name);

    void createIndexForCollection();

    String getCollectionName();

    void insertDocumentToCollection(List<Map<String, Object>> docs, int numDocumentsPerInsert);

    long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime,
                                int thisCustomerId, int queryLimit);
}
