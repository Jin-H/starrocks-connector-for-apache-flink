package com.starrocks.connector.flink.table.source;

import java.io.Serializable;

public class LookupConfig implements Serializable {

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public LookupConfig(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }
}
