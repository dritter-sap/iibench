package iibench.data;

import java.util.List;
import java.util.Map;

public interface DataGenerator {
    List<Map<String, Object>> generateBatch(int batchSize, int numCustomers, int numCashRegisters, int numProducts, double maxPrice);
}
