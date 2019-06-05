package com.netflix.titus.master.mesos;

public class LeaseRescindedEvent {

    private static LeaseRescindedEvent ALL_EVENT = new LeaseRescindedEvent(Type.All, "");

    public enum Type {
        All,
        LeaseId,
        Hostname
    }

    private final Type type;
    private final String value;

    private LeaseRescindedEvent(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public static LeaseRescindedEvent allEvent() {
        return ALL_EVENT;
    }

    public static LeaseRescindedEvent leaseIdEvent(String leaseId) {
        return new LeaseRescindedEvent(Type.LeaseId, leaseId);
    }

    public static LeaseRescindedEvent hostnameEvent(String hostname) {
        return new LeaseRescindedEvent(Type.Hostname, hostname);
    }
}
