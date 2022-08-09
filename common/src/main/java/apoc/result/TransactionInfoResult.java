package apoc.result;

public class TransactionInfoResult {

    public long rolledBackTx;

    public long peakTx;

    public long lastTxId;

    public long currentOpenedTx;

    public long totalOpenedTx;

    public long totalTx;

    public TransactionInfoResult(
            long rolledBackTx,
            long peakTx,
            long lastTxId,
            long currentOpenedTx,
            long totalOpenedTx,
            long totalTx
    ) {
        this.rolledBackTx = rolledBackTx;
        this.peakTx = peakTx;
        this.lastTxId = lastTxId;
        this.currentOpenedTx = currentOpenedTx;
        this.totalOpenedTx = totalOpenedTx;
        this.totalTx = totalTx;
    }

}
