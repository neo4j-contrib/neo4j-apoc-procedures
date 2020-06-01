package apoc.result;

import java.text.SimpleDateFormat;
import java.util.Date;

public class KernelInfoResult {

    public Boolean readOnly;

    public String kernelVersion;

    public String storeId;

    public String kernelStartTime;

    public String databaseName;

    public long storeLogVersion;

    public String storeCreationDate;

    public KernelInfoResult(
            Boolean readOnly,
            String kernelVersion,
            String storeId,
            Date kernelStartTime,
            String databaseName,
            long storeLogVersion,
            Date storeCreationDate) {

        SimpleDateFormat format = new SimpleDateFormat(apoc.date.Date.DEFAULT_FORMAT);

        this.readOnly = readOnly;
        this.kernelVersion = kernelVersion;
        this.storeId = storeId;
        this.kernelStartTime = format.format(kernelStartTime);
        this.databaseName = databaseName;
        this.storeLogVersion = storeLogVersion;
        this.storeCreationDate = format.format(storeCreationDate);
    }

}
