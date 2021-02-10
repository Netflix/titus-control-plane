package com.netflix.titus.runtime.connector.eviction;

public enum EvictionRejectionReasons {
    LIMIT_EXCEEDED("System eviction quota limit exceeded"),
    SYSTEM_WINDOW_CLOSED("Outside system time window");

    private String reasonMessage;

    EvictionRejectionReasons(String reasonMessage) {
        this.reasonMessage = reasonMessage;
    }

    public String getReasonMessage() {
        return reasonMessage;
    }
}
