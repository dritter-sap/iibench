package iibench.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Deprecated
public class FixedDataGenerator implements DataGenerator {
    @Override
    public List<Map<String, Object>> generateBatch(int batchSize, int numCustomers, int numCashRegisters, int numProducts, double maxPrice) {
        final List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            final Map<String, Object> map = new HashMap<>();
            final int thisCustomerId = numCustomers;
            map.put("dateandtime", 4711);
            map.put("cashregisterid", numCashRegisters);
            map.put("customerid", thisCustomerId);
            map.put("productid", numProducts);
            map.put("price", maxPrice);
            docs.add(map);
        }
        return docs;
    }
}
