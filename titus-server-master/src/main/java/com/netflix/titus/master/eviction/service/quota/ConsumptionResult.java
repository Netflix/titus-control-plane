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

package com.netflix.titus.master.eviction.service.quota;

import java.util.Objects;
import java.util.Optional;

public class ConsumptionResult {

    private static final ConsumptionResult APPROVED = new ConsumptionResult(true, Optional.empty());

    private final boolean approved;
    private final Optional<String> rejectionReason;

    private ConsumptionResult(boolean approved, Optional<String> rejectionReason) {
        this.approved = approved;
        this.rejectionReason = rejectionReason;
    }

    public boolean isApproved() {
        return approved;
    }

    public Optional<String> getRejectionReason() {
        return rejectionReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumptionResult that = (ConsumptionResult) o;
        return approved == that.approved &&
                Objects.equals(rejectionReason, that.rejectionReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(approved, rejectionReason);
    }

    @Override
    public String toString() {
        return "ConsumptionResult{" +
                "approved=" + approved +
                ", rejectionReason=" + rejectionReason +
                '}';
    }

    public static ConsumptionResult approved() {
        return APPROVED;
    }

    public static ConsumptionResult rejected(String rejectionReason) {
        return new ConsumptionResult(false, Optional.of(rejectionReason));
    }
}
