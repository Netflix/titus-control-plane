/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.connector;

import com.google.common.base.Preconditions;

public class TitusNode {

    private final String id;
    private final String serverGroupId;

    private String ipAddress;
    private boolean relocationNotAllowed;
    private boolean relocationRequired;
    private boolean relocationRequiredImmediately;
    private boolean serverGroupRelocationRequired;
    private boolean inBadCondition;

    public TitusNode(String id,
                     String serverGroupId) {
        this.id = id;
        this.serverGroupId = serverGroupId;
    }

    public String getId() {
        return id;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getServerGroupId() {
        return serverGroupId;
    }

    public boolean isRelocationNotAllowed() {
        return relocationNotAllowed;
    }

    public boolean isRelocationRequired() {
        return relocationRequired;
    }

    public boolean isRelocationRequiredImmediately() {
        return relocationRequiredImmediately;
    }

    public boolean isServerGroupRelocationRequired() {
        return serverGroupRelocationRequired;
    }

    public boolean isInBadCondition() {
        return inBadCondition;
    }

    public Builder toBuilder() {
        return newBuilder()
                .withId(id)
                .withServerGroupId(serverGroupId)
                .withRelocationRequired(relocationRequired)
                .withRelocationRequiredImmediately(relocationRequiredImmediately)
                .withRelocationNotAllowed(relocationNotAllowed)
                .withServerGroupRelocationRequired(serverGroupRelocationRequired);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String serverGroupId;
        private boolean relocationRequired;
        private boolean relocationNotAllowed;
        private boolean relocationRequiredImmediately;
        private boolean serverGroupRelocationRequired;
        private boolean inBadCondition;
        private String ipAddress;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withServerGroupId(String serverGroupId) {
            this.serverGroupId = serverGroupId;
            return this;
        }

        public Builder withRelocationNotAllowed(boolean relocationNotAllowed) {
            this.relocationNotAllowed = relocationNotAllowed;
            return this;
        }

        public Builder withRelocationRequired(boolean relocationRequired) {
            this.relocationRequired = relocationRequired;
            return this;
        }

        public Builder withRelocationRequiredImmediately(boolean relocationRequiredImmediately) {
            this.relocationRequiredImmediately = relocationRequiredImmediately;
            return this;
        }

        public Builder withServerGroupRelocationRequired(boolean serverGroupRelocationRequired) {
            this.serverGroupRelocationRequired = serverGroupRelocationRequired;
            return this;
        }

        public Builder withBadCondition(boolean inBadCondition) {
            this.inBadCondition = inBadCondition;
            return this;
        }

        public TitusNode build() {
            Preconditions.checkNotNull(id, "instance id is null");
            Preconditions.checkNotNull(serverGroupId, "server group id is null");
            TitusNode node = new TitusNode(id, serverGroupId);
            node.ipAddress = ipAddress;
            node.relocationNotAllowed = this.relocationNotAllowed;
            node.relocationRequiredImmediately = this.relocationRequiredImmediately;
            node.relocationRequired = this.relocationRequired;
            node.serverGroupRelocationRequired = this.serverGroupRelocationRequired;
            node.inBadCondition = this.inBadCondition;
            return node;
        }
    }
}
