package com.fuzzy.common.query;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class QueryExecutionStatistical {
    private final AtomicLong successQueryCounter = new AtomicLong();
    private final AtomicLong errorQueryCounter = new AtomicLong();
    private final AtomicLong invalidQueryCounter = new AtomicLong();
    private final AtomicLong calculateTimeOverhead = new AtomicLong();

    public long incrementSuccessQueryCount() {
        return successQueryCounter.incrementAndGet();
    }

    public long incrementErrorQueryCount() {
        return errorQueryCounter.incrementAndGet();
    }

    public long incrementInvalidQueryCount() {
        return invalidQueryCounter.incrementAndGet();
    }

    public long incrementCalculateTimeOverhead(long calculateTimeOverhead) {
        return this.calculateTimeOverhead.getAndAdd(calculateTimeOverhead);
    }

    @Override
    public String toString() {
        return "QueryExecutionStatistical{" +
                "successQueryCounter=" + successQueryCounter +
                ", errorQueryCounter=" + errorQueryCounter +
                ", invalidQueryCounter=" + invalidQueryCounter +
                ", calculateTimeOverhead=" + calculateTimeOverhead +
                '}';
    }

    public enum QueryExecutionType {
        success, error, invalid
    }

}
