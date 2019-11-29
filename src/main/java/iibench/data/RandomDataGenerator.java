package iibench.data;

import java.util.*;

public class RandomDataGenerator implements DataGenerator {
    private final Random rand;

    public RandomDataGenerator(final int threadNumber) {
        rand = new java.util.Random((long) threadNumber);
    }

    public List<Map<String, Object>> generateBatch(final int batchSize, final int numCustomers, final int numCashRegisters,
                                                   final int numProducts, final double maxPrice) {
        if (rand == null) {
            throw new IllegalStateException("Data generator must be initialized.");
        }
        final List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            final Map<String, Object> map = new HashMap<>();
            final int thisCustomerId = rand.nextInt(numCustomers);
            map.put("dateandtime", System.currentTimeMillis());
            map.put("cashregisterid", rand.nextInt(numCashRegisters));
            map.put("customerid", thisCustomerId);
            map.put("productid", rand.nextInt(numProducts));
            map.put("price", ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0);
            docs.add(map);
        }
        return docs;
    }
}
