package apoc.result;

import java.util.List;
import java.util.Map;

/**
 * @author kv
 * @since 6.05.16
 */
public class LockInfoResult {

    public long advertedDeadLocks;
	
	public long lockCount;

    public String info;
	
	public long contendedLockCount;
	
    public List<Map<String,Object>> contendedLocks;
    
    public LockInfoResult(
    		long mwt,
            long locksCount,
            long advertedDeadLocks,
            long contendedLockCount,
            List<Map<String,Object>> lockinfos
    ) {
        this.advertedDeadLocks = advertedDeadLocks;  
    	this.lockCount = locksCount;
    	this.contendedLockCount = contendedLockCount;
        this.contendedLocks = lockinfos;   
        this.info = "Showing contended locks where threads have waited for at least " + mwt + " ms.";  
    }

}
