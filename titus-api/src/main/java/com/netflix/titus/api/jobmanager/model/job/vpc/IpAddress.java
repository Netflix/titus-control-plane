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
 * IP Address to be assigned to task.
 */
public class IpAddress {

    @Size(min = 1, message = "Emtpy value not allowed")
    private final String address;

    private IpAddress(String address) {
        this.address = address;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IpAddress ipAddress = (IpAddress) o;
        return address.equals(ipAddress.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }

    @Override
    public String toString() {
        return "IpAddress{" +
                "address='" + address + '\'' +
                '}';
    }

    public static final class Builder {
        private String address;

        private Builder() {
        }

        public Builder withAddress(String val) {
            address = val;
            return this;
        }

        public IpAddress build() {
            return new IpAddress(address);
        }
    }
}
