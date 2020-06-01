package apoc.export.util;

import apoc.result.ProgressInfo;

/**
 * @author mh
 * @since 22.05.16
 */
public interface Reporter {
    void progress(String msg);
    void update(long nodes, long rels, long properties);

    void done();

    ProgressInfo getTotal();

    void nextRow();
}
