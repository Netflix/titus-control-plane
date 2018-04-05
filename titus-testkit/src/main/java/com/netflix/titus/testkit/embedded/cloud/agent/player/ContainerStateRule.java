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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.Objects;
import java.util.Optional;

class ContainerStateRule {

    private final long delayInStateMs;
    private final Optional<String> reasonCode;
    private final Optional<String> reasonMessage;

    ContainerStateRule(long delayInStateMs, Optional<String> reasonCode, Optional<String> reasonMessage) {
        this.delayInStateMs = delayInStateMs;
        this.reasonCode = reasonCode;
        this.reasonMessage = reasonMessage;
    }

    public long getDelayInStateMs() {
        return delayInStateMs;
    }

    public Optional<String> getReasonCode() {
        return reasonCode;
    }

    public Optional<String> getReasonMessage() {
        return reasonMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerStateRule that = (ContainerStateRule) o;
        return delayInStateMs == that.delayInStateMs &&
                Objects.equals(reasonCode, that.reasonCode) &&
                Objects.equals(reasonMessage, that.reasonMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delayInStateMs, reasonCode, reasonMessage);
    }

    @Override
    public String toString() {
        return "ContainerStateRule{" +
                "delayInStateMs=" + delayInStateMs +
                ", reasonCode=" + reasonCode +
                ", reasonMessage=" + reasonMessage +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private long delayInStateMs;
        private Optional<String> reasonCode = Optional.empty();
        private Optional<String> reasonMessage = Optional.empty();

        private Builder() {
        }

        public Builder withDelayInStateMs(long delayInStateMs) {
            this.delayInStateMs = delayInStateMs;
            return this;
        }

        public Builder withReason(String reasonCode, String reasonMessage) {
            this.reasonCode = Optional.of(reasonCode);
            this.reasonMessage = Optional.of(reasonMessage);
            return this;
        }

        public ContainerStateRule build() {
            return new ContainerStateRule(delayInStateMs, reasonCode, reasonMessage);
        }
    }
}
