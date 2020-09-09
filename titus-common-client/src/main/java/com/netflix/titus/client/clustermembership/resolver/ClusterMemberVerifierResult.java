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

package com.netflix.titus.client.clustermembership.resolver;

import java.util.Objects;

public class ClusterMemberVerifierResult {

    private static final ClusterMemberVerifierResult VALID = new ClusterMemberVerifierResult(true, "valid");

    private final boolean valid;
    private final String message;

    public ClusterMemberVerifierResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public boolean isValid() {
        return valid;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMemberVerifierResult result = (ClusterMemberVerifierResult) o;
        return valid == result.valid &&
                Objects.equals(message, result.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valid, message);
    }

    @Override
    public String toString() {
        return "ClusterMemberVerifierResult{" +
                "valid=" + valid +
                ", message='" + message + '\'' +
                '}';
    }

    public static ClusterMemberVerifierResult valid() {
        return VALID;
    }

    public static ClusterMemberVerifierResult invalid(String errorMessage) {
        return new ClusterMemberVerifierResult(false, errorMessage);
    }
}
