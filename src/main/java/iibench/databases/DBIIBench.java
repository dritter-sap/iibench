package iibench.databases;

import java.util.List;
import java.util.Map;

public interface DBIIBench {
    void connect() throws Exception;

    void disconnect();

    void checkIndexUsed();

    void createCollection(String name);

    void createIndexForCollection();

    String getCollectionName();

    void insertDocumentToCollection(List<Map<String, Object>> docs);

    long queryAndMeasureElapsed(int whichQuery, double thisPrice, int thisCashRegisterId, long thisRandomTime, int thisCustomerId);
}
