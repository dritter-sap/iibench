package iibench;

import iibench.databases.HDBDocStoreIIBench;
import iibench.databases.MongoIIBench;
import iibench.databases.MongoIIBenchOldAPI;
import iibench.databases.OrientIIBench;

import java.util.HashMap;
import java.util.Map;

public class DatabaseTypes {
    private static final Map<String, DBIIBench> databases = new HashMap<String, DBIIBench>() {{
        put("docstore", new HDBDocStoreIIBench());
        put("orientdb", new OrientIIBench());
        put("mongodb", new MongoIIBench());
        put("mongodbold", new MongoIIBenchOldAPI());
    }};

    public static DBIIBench select(final String dbType) {
        if (!databases.containsKey(dbType)) {
            throw new IllegalArgumentException("Database type " + dbType + " unknown.");
        }
        return databases.get(dbType);
    }
}
