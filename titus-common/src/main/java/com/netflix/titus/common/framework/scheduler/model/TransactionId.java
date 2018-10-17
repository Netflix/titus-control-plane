package com.netflix.titus.common.framework.scheduler.model;

import java.util.Objects;

public class TransactionId {

    private static final TransactionId INITIAL = new TransactionId(1, 0, 1);

    private final int major;
    private final int retry;
    private final int total;

    public TransactionId(int major, int retry, int total) {
        this.major = major;
        this.retry = retry;
        this.total = total;
    }

    public int getMajor() {
        return major;
    }

    public int getRetry() {
        return retry;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionId that = (TransactionId) o;
        return major == that.major &&
                retry == that.retry &&
                total == that.total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, retry, total);
    }

    @Override
    public String toString() {
        return "TransactionId{" +
                "major=" + major +
                ", retry=" + retry +
                ", total=" + total +
                '}';
    }

    public static TransactionId initial() {
        return INITIAL;
    }

    public static TransactionId nextMajor(TransactionId transactionId) {
        return new TransactionId(transactionId.getMajor() + 1, transactionId.getRetry(), transactionId.getTotal() + 1);
    }

    public static TransactionId nextRetried(TransactionId transactionId) {
        return new TransactionId(transactionId.getMajor(), transactionId.getRetry() + 1, transactionId.getTotal() + 1);
    }
}
