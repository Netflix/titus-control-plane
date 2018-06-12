package com.netflix.titus.api.eviction.model;

import java.util.Objects;

import com.netflix.titus.api.model.reference.Reference;

public class EvictionQuota {

    private final Reference reference;
    private final long quota;

    public EvictionQuota(Reference reference, long quota) {
        this.reference = reference;
        this.quota = quota;
    }

    public Reference getReference() {
        return reference;
    }

    public long getQuota() {
        return quota;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EvictionQuota that = (EvictionQuota) o;
        return quota == that.quota &&
                Objects.equals(reference, that.reference);
    }

    @Override
    public int hashCode() {

        return Objects.hash(reference, quota);
    }

    @Override
    public String toString() {
        return "EvictionQuota{" +
                "reference=" + reference +
                ", quota=" + quota +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Reference reference;
        private long quota;

        private Builder() {
        }

        public Builder withReference(Reference reference) {
            this.reference = reference;
            return this;
        }

        public Builder withQuota(long quota) {
            this.quota = quota;
            return this;
        }

        public EvictionQuota build() {
            return new EvictionQuota(reference, quota);
        }
    }
}
