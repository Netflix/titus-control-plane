/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.api.supervisor.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.Evaluators;

public class MasterInstance {

    private final String instanceId;
    private final String instanceGroupId;
    private final String ipAddress;
    private final MasterStatus status;
    private final List<MasterStatus> statusHistory;
    private final List<ServerPort> serverPorts;
    private final Map<String, String> labels;

    public MasterInstance(String instanceId,
                          String instanceGroupId,
                          String ipAddress,
                          MasterStatus status,
                          List<MasterStatus> statusHistory,
                          List<ServerPort> serverPorts,
                          Map<String, String> labels) {
        this.instanceId = instanceId;
        this.instanceGroupId = instanceGroupId;
        this.ipAddress = ipAddress;
        this.status = status;
        this.statusHistory = statusHistory;
        this.serverPorts = serverPorts;
        this.labels = labels;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getInstanceGroupId() {
        return instanceGroupId;
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

    public List<ServerPort> getServerPorts() {
        return serverPorts;
    }

    public Map<String, String> getLabels() {
        return labels;
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
                Objects.equals(instanceGroupId, that.instanceGroupId) &&
                Objects.equals(ipAddress, that.ipAddress) &&
                Objects.equals(status, that.status) &&
                Objects.equals(statusHistory, that.statusHistory) &&
                Objects.equals(serverPorts, that.serverPorts) &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, instanceGroupId, ipAddress, status, statusHistory, serverPorts, labels);
    }

    @Override
    public String toString() {
        return "MasterInstance{" +
                "instanceId='" + instanceId + '\'' +
                ", instanceGroupId='" + instanceGroupId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                ", serverPorts=" + serverPorts +
                ", labels=" + labels +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withInstanceId(instanceId)
                .withInstanceGroupId(instanceGroupId)
                .withIpAddress(ipAddress)
                .withStatus(status)
                .withStatusHistory(statusHistory)
                .withServerPorts(serverPorts)
                .withLabels(labels);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String instanceId;
        private String instanceGroupId;
        private String ipAddress;
        private MasterStatus status;
        private List<MasterStatus> statusHistory;
        private List<ServerPort> serverPorts;
        private Map<String, String> labels;

        private Builder() {
        }

        public Builder withInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder withInstanceGroupId(String instanceGroupId) {
            this.instanceGroupId = instanceGroupId;
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

        public Builder withServerPorts(List<ServerPort> serverPorts) {
            this.serverPorts = serverPorts;
            return this;
        }

        public Builder withLabels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public MasterInstance build() {
            Preconditions.checkNotNull(instanceId, "Instance id not set");
            Preconditions.checkNotNull(instanceGroupId, "Instance group id not set");
            Preconditions.checkNotNull(ipAddress, "ip address not set");
            Preconditions.checkNotNull(status, "status not set");

            this.statusHistory = Evaluators.getOrDefault(statusHistory, Collections.emptyList());
            this.serverPorts = Evaluators.getOrDefault(serverPorts, Collections.emptyList());
            this.labels = Evaluators.getOrDefault(labels, Collections.emptyMap());
            return new MasterInstance(instanceId, instanceGroupId, ipAddress, status, statusHistory, serverPorts, labels);
        }
    }
}
