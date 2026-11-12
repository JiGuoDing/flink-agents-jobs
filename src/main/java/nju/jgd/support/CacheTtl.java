package nju.jgd.support;

/** Timestamp-based freshness check for the logical response cache. */
public final class CacheTtl {

    private CacheTtl() {}

    public static boolean isFresh(long createdAt, long now, long ttlMillis) {
        if (ttlMillis <= 0L || createdAt <= 0L || now < createdAt) {
            return false;
        }
        return now - createdAt <= ttlMillis;
    }
}
