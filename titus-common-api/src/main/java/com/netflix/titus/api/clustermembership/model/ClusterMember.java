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

package com.netflix.titus.api.clustermembership.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.Evaluators;

public class ClusterMember {

    private final String memberId;
    private final boolean enabled;
    private final boolean registered;
    private final boolean active;
    private final List<ClusterMemberAddress> clusterMemberAddresses;
    private final Map<String, String> labels;

    public ClusterMember(String memberId,
                         boolean enabled,
                         boolean registered,
                         boolean active,
                         List<ClusterMemberAddress> clusterMemberAddresses,
                         Map<String, String> labels) {
        this.memberId = memberId;
        this.enabled = enabled;
        this.registered = registered;
        this.active = active;
        this.clusterMemberAddresses = Evaluators.getOrDefault(clusterMemberAddresses, Collections.emptyList());
        this.labels = Evaluators.getOrDefault(labels, Collections.emptyMap());
    }

    public String getMemberId() {
        return memberId;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRegistered() {
        return registered;
    }

    public boolean isActive() {
        return active;
    }

    public List<ClusterMemberAddress> getClusterMemberAddresses() {
        return clusterMemberAddresses;
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
        ClusterMember that = (ClusterMember) o;
        return enabled == that.enabled &&
                registered == that.registered &&
                active == that.active &&
                Objects.equals(memberId, that.memberId) &&
                Objects.equals(clusterMemberAddresses, that.clusterMemberAddresses) &&
                Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, enabled, registered, active, clusterMemberAddresses, labels);
    }

    @Override
    public String toString() {
        return "ClusterMember{" +
                "memberId='" + memberId + '\'' +
                ", enabled=" + enabled +
                ", registered=" + registered +
                ", active=" + active +
                ", clusterMemberAddresses=" + clusterMemberAddresses +
                ", labels=" + labels +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder()
                .withMemberId(memberId)
                .withEnabled(enabled)
                .withRegistered(registered)
                .withActive(active)
                .withClusterMemberAddresses(clusterMemberAddresses)
                .withLabels(labels);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String memberId;
        private boolean enabled;
        private boolean registered;
        private boolean active;
        private List<ClusterMemberAddress> clusterMemberAddress;
        private Map<String, String> labels;

        private Builder() {
        }

        public Builder withMemberId(String memberId) {
            this.memberId = memberId;
            return this;
        }

        public Builder withEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder withRegistered(boolean registered) {
            this.registered = registered;
            return this;
        }

        public Builder withActive(boolean active) {
            this.active = active;
            return this;
        }

        public Builder withClusterMemberAddresses(List<ClusterMemberAddress> clusterMemberAddress) {
            this.clusterMemberAddress = clusterMemberAddress;
            return this;
        }

        public Builder withLabels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public ClusterMember build() {
            Preconditions.checkNotNull(memberId, "Member id is null");

            return new ClusterMember(memberId, enabled, registered, active, clusterMemberAddress, labels);
        }
    }
}
