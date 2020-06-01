package apoc.export.util;

/**
 * @author mh
 * @since 22.05.16
 */
interface SizeCounter {
    long getNewLines();
    long getCount();
    long getTotal();
    long getPercent();
}
