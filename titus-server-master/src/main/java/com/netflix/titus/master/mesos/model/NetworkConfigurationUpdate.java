package com.netflix.titus.master.mesos.model;

import java.util.Objects;

/**
 * Container status entity that carries over network related data.
 */
public class NetworkConfigurationUpdate implements ContainerStatus {

    private final String ipAddress;
    private final String eniId;

    public NetworkConfigurationUpdate(String ipAddress, String eniId) {
        this.ipAddress = ipAddress;
        this.eniId = eniId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getEniId() {
        return eniId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NetworkConfigurationUpdate that = (NetworkConfigurationUpdate) o;
        return Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(eniId, that.eniId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, eniId);
    }

    @Override
    public String toString() {
        return "NetworkConfigurationUpdate{" +
                "ipAddress='" + ipAddress + '\'' +
                ", eniId='" + eniId + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String ipAddress;
        private String eniId;

        private Builder() {
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withEniId(String eniId) {
            this.eniId = eniId;
            return this;
        }

        public Builder but() {
            return newBuilder().withIpAddress(ipAddress).withEniId(eniId);
        }

        public NetworkConfigurationUpdate build() {
            return new NetworkConfigurationUpdate(ipAddress, eniId);
        }
    }
}
