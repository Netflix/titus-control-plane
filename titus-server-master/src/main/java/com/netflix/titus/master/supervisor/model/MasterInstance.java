package com.netflix.titus.master.supervisor.model;

import java.util.List;
import java.util.Objects;

public class MasterInstance {

    private final String instanceId;
    private final String ipAddress;
    private final MasterStatus status;
    private final List<MasterStatus> statusHistory;

    public MasterInstance(String instanceId, String ipAddress, MasterStatus status, List<MasterStatus> statusHistory) {
        this.instanceId = instanceId;
        this.ipAddress = ipAddress;
        this.status = status;
        this.statusHistory = statusHistory;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public MasterStatus getStatus() {
        return status;
    }

    public List<MasterStatus> getStatusHistory() {
        return statusHistory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MasterInstance that = (MasterInstance) o;
        return Objects.equals(instanceId, that.instanceId) &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(status, that.status) &&
                Objects.equals(statusHistory, that.statusHistory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, ipAddress, status, statusHistory);
    }

    @Override
    public String toString() {
        return "MasterInstance{" +
                "instanceId='" + instanceId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withInstanceId(instanceId).withIpAddress(ipAddress).withStatus(status).withStatusHistory(statusHistory);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String instanceId;
        private String ipAddress;
        private MasterStatus status;
        private List<MasterStatus> statusHistory;

        private Builder() {
        }

        public Builder withInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withStatus(MasterStatus status) {
            this.status = status;
            return this;
        }

        public Builder withStatusHistory(List<MasterStatus> statusHistory) {
            this.statusHistory = statusHistory;
            return this;
        }

        public Builder but() {
            return newBuilder().withInstanceId(instanceId).withIpAddress(ipAddress).withStatus(status).withStatusHistory(statusHistory);
        }

        public MasterInstance build() {
            return new MasterInstance(instanceId, ipAddress, status, statusHistory);
        }
    }
}
