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

/**
 * Location within a cloud provider of an IP address
 */
public class IpAddressLocation {

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String region;

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String availabilityZone;

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String subnetId;

    public String getRegion() {
        return region;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getSubnetId() {
        return subnetId;
    }

    private IpAddressLocation(String region, String availabilityZone, String subnetId) {
        this.region = region;
        this.availabilityZone = availabilityZone;
        this.subnetId = subnetId;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IpAddressLocation that = (IpAddressLocation) o;
        return Objects.equals(region, that.region) &&
                Objects.equals(availabilityZone, that.availabilityZone) &&
                Objects.equals(subnetId, that.subnetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, availabilityZone, subnetId);
    }

    @Override
    public String toString() {
        return "IpAddressLocation{" +
                "region='" + region + '\'' +
                ", availabilityZone='" + availabilityZone + '\'' +
                ", subnetId='" + subnetId + '\'' +
                '}';
    }

    public static final class Builder {
        private String region;
        private String availabilityZone;
        private String subnetId;

        private Builder() {
        }

        public Builder withRegion(String val) {
            region = val;
            return this;
        }

        public Builder withAvailabilityZone(String val) {
            availabilityZone = val;
            return this;
        }

        public Builder withSubnetId(String val) {
            subnetId = val;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withRegion(region)
                    .withAvailabilityZone(availabilityZone)
                    .withSubnetId(subnetId);
        }

        public IpAddressLocation build() {
            return new IpAddressLocation(region, availabilityZone, subnetId);
        }
    }
}
