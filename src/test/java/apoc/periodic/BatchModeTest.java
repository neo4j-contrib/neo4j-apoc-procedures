package apoc.periodic;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchModeTest {
    @Test
    public void useBatchModeIfDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", BatchMode.BATCH_SINGLE.name());

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.BATCH_SINGLE, batchMode);
    }

    @Test
    public void useIterateListIfBatchModeNotDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("iterateList", false);

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.SINGLE, batchMode);
    }

    @Test
    public void batchModeByDefault() {
        BatchMode batchMode = BatchMode.fromConfig(new HashMap<>());
        assertEquals(BatchMode.BATCH, batchMode);
    }

    @Test
    public void ignoreCaseOfBatchMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", BatchMode.BATCH_SINGLE.name().toLowerCase());

        BatchMode batchMode = BatchMode.fromConfig(config);
        assertEquals(BatchMode.BATCH_SINGLE, batchMode);
    }

    @Test
    public void throwExceptionForInvalidBatchMode() {
        Map<String, Object> config = new HashMap<>();
        config.put("batchMode", "random");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            BatchMode.fromConfig(config);
        });

        assertEquals("Invalid batch mode: `random`. Valid values are: [BATCH, BATCH_SINGLE, SINGLE]", exception.getMessage());
    }
}