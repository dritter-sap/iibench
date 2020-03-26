package iibench;

import iibench.databases.*;

import java.util.HashMap;
import java.util.Map;

public class DatabaseTypes {
    private static final Map<String, DBIIBench> databases = new HashMap<String, DBIIBench>() {{
        put("docstore", new HDBDocStoreIIBench());
        put("orientdb", new OrientIIBench());
        put("mongodb", new MongoIIBench());
        put("mongodbold", new MongoIIBenchOldAPI());
        put("orientdbhash", new OrientHashIIBench());
        put("mongodbhash", new MongoHashIIBench());

        put("docstore_simple", new HDBDocStoreSimpleIIBench());
        put("orientdbhash_simple", new OrientHashSimpleIIBench());
        put("mongodbhash_simple", new MongoHashSimpleIIBench());
    }};

    public static DBIIBench select(final String dbType) {
        if (!databases.containsKey(dbType)) {
            throw new IllegalArgumentException("Database type " + dbType + " unknown.");
        }
        return databases.get(dbType);
    }
}
