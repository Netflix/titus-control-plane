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

package com.netflix.titus.api.jobmanager.model.job.vpc;

import java.util.Arrays;
import java.util.Objects;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

@ClassFieldsNotNull
public class SignedIpAddressAllocation {

    private final IpAddressAllocation ipAddressAllocation;

    private final byte[] ipAddressAllocationSignature;

    public SignedIpAddressAllocation(IpAddressAllocation ipAddressAllocation, byte[] ipAddressAllocationSignature) {
        this.ipAddressAllocation = ipAddressAllocation;
        this.ipAddressAllocationSignature = ipAddressAllocationSignature;
    }

    public IpAddressAllocation getIpAddressAllocation() {
        return ipAddressAllocation;
    }

    public byte[] getIpAddressAllocationSignature() {
        return ipAddressAllocationSignature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SignedIpAddressAllocation that = (SignedIpAddressAllocation) o;
        return Objects.equals(ipAddressAllocation, that.ipAddressAllocation) &&
                Arrays.equals(ipAddressAllocationSignature, that.ipAddressAllocationSignature);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ipAddressAllocation);
        result = 31 * result + Arrays.hashCode(ipAddressAllocationSignature);
        return result;
    }

    @Override
    public String toString() {
        return "SignedIpAddressAllocation{" +
                "ipAddressAllocation=" + ipAddressAllocation +
                ", ipAddressAllocationSignature=" + Arrays.toString(ipAddressAllocationSignature) +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {
        private IpAddressAllocation ipAddressAllocation;
        private byte[] ipAddressAllocationSignature;

        private Builder() {
        }

        public Builder withIpAddressAllocation(IpAddressAllocation val) {
            ipAddressAllocation = val;
            return this;
        }

        public Builder withIpAddressAllocationSignature(byte[] val) {
            ipAddressAllocationSignature = val;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withIpAddressAllocation(ipAddressAllocation)
                    .withIpAddressAllocationSignature(ipAddressAllocationSignature);
        }

        public SignedIpAddressAllocation build() {
            return new SignedIpAddressAllocation(ipAddressAllocation, ipAddressAllocationSignature);
        }
    }
}
