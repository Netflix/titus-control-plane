/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.supervisor.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.Evaluators;

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
            Preconditions.checkNotNull(instanceId, "Instance id not set");
            Preconditions.checkNotNull(ipAddress, "ip address not set");
            Preconditions.checkNotNull(status, "status not set");

            this.statusHistory = Evaluators.getOrDefault(statusHistory, Collections.emptyList());
            return new MasterInstance(instanceId, ipAddress, status, statusHistory);
        }
    }
}
