package apoc.result;

import apoc.monitor.KernelInfo;

public class KernelInfoResult {

    public Boolean readOnly;

    public String kernelVersion;

    public String storeId;

    public long kernelStartTime;

    public String databaseName;

    public long storeLogVersion;

    public long storeCreationDate;

    public KernelInfoResult(
            Boolean readOnly,
            String kernelVersion,
            String storeId,
            long kernelStartTime,
            String databaseName,
            long storeLogVersion,
            long storeCreationDate
            ) {
        this.readOnly = readOnly;
        this.kernelVersion = kernelVersion;
        this.storeId = storeId;
        this.kernelStartTime = kernelStartTime;
        this.databaseName = databaseName;
        this.storeLogVersion = storeLogVersion;
        this.storeCreationDate = storeCreationDate;
    }

}
