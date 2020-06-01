package apoc.periodic;

import apoc.util.Util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

enum BatchMode {
    BATCH,
    BATCH_SINGLE,
    SINGLE;


    private static final Map<String, BatchMode> nameIndex =  new HashMap<>();
    static {
        for (BatchMode batchMode : BatchMode.values()) {
            nameIndex.put(batchMode.name(), batchMode);
        }
    }

   static BatchMode fromIterateList(boolean iterateList) {
        return iterateList ? BATCH : SINGLE;
    }

    static BatchMode fromConfig(Map<String,Object> config) {
        Object batchMode = config.get("batchMode");

        if(batchMode != null) {
            BatchMode lookedUpBatchMode = nameIndex.get(batchMode.toString().toUpperCase());
            if (lookedUpBatchMode == null) {
                throw new IllegalArgumentException("Invalid batch mode: `" + batchMode + "`. Valid values are: " + Arrays.toString(BatchMode.values()));
            }
            return lookedUpBatchMode;
        }

        Object iterateList = config.get("iterateList");
        if(iterateList != null) {
            return BatchMode.fromIterateList(Util.toBoolean(iterateList));
        }

        return BATCH;
    }
}
