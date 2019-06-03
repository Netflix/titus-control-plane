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

import java.util.Objects;
import javax.validation.constraints.Size;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

/**
 * VPC IP address allocated from Titus IP Service
 */
@ClassFieldsNotNull
public class IpAddressAllocation {

    private final IpAddressLocation ipAddressLocation;

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String allocationId;

    private final IpAddress ipAddress;

    public IpAddressAllocation(IpAddressLocation ipAddressLocation, String allocationId, IpAddress ipAddress) {
        this.ipAddressLocation = ipAddressLocation;
        this.allocationId = allocationId;
        this.ipAddress = ipAddress;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public IpAddressLocation getIpAddressLocation() {
        return ipAddressLocation;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public IpAddress getIpAddress() {
        return ipAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IpAddressAllocation that = (IpAddressAllocation) o;
        return Objects.equals(ipAddressLocation, that.ipAddressLocation) &&
                Objects.equals(allocationId, that.allocationId) &&
                Objects.equals(ipAddress, that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddressLocation, allocationId, ipAddress);
    }

    @Override
    public String toString() {
        return "IpAddressAllocation{" +
                "ipAddressLocation=" + ipAddressLocation +
                ", allocationId='" + allocationId + '\'' +
                ", ipAddress=" + ipAddress +
                '}';
    }

    public static final class Builder {
        private IpAddressLocation ipAddressLocation;
        private String allocationId;
        private IpAddress ipAddress;

        private Builder() {
        }

        public Builder withIpAddressLocation(IpAddressLocation val) {
            ipAddressLocation = val;
            return this;
        }

        public Builder withUuid(String val) {
            allocationId = val;
            return this;
        }

        public Builder withIpAddress(IpAddress val) {
            ipAddress = val;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withIpAddressLocation(ipAddressLocation)
                    .withUuid(allocationId)
                    .withIpAddress(ipAddress);
        }

        public IpAddressAllocation build() {
            return new IpAddressAllocation(ipAddressLocation, allocationId, ipAddress);
        }
    }
}
