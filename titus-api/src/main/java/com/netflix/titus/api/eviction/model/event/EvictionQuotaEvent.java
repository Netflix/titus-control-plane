package com.netflix.titus.api.eviction.model.event;

import java.util.Objects;

import com.netflix.titus.api.eviction.model.EvictionQuota;

public class EvictionQuotaEvent extends EvictionEvent {

    private final EvictionQuota quota;

    public EvictionQuotaEvent(EvictionQuota quota) {
        this.quota = quota;
    }

    public EvictionQuota getQuota() {
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
        EvictionQuotaEvent that = (EvictionQuotaEvent) o;
        return Objects.equals(quota, that.quota);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quota);
    }

    @Override
    public String toString() {
        return "EvictionQuotaEvent{" +
                "quota=" + quota +
                "} " + super.toString();
    }
}
