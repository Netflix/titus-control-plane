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

package com.netflix.titus.api.model.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceJobProcesses {
    private static Logger logger = LoggerFactory.getLogger(ServiceJobProcesses.class);
    private boolean disableIncreaseDesired;

    private boolean disableDecreaseDesired;

    @JsonCreator
    public ServiceJobProcesses(@JsonProperty("disableIncreaseDesired") boolean disableIncreaseDesired,
                               @JsonProperty("disableDecreaseDesired") boolean disableDecreaseDesired) {
        this.disableIncreaseDesired = disableIncreaseDesired;
        this.disableDecreaseDesired = disableDecreaseDesired;
    }

    public boolean isDisableIncreaseDesired() {
        return disableIncreaseDesired;
    }

    public boolean isDisableDecreaseDesired() {
        return disableDecreaseDesired;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceJobProcesses that = (ServiceJobProcesses) o;

        if (disableIncreaseDesired != that.disableIncreaseDesired) {
            return false;
        }
        return disableDecreaseDesired == that.disableDecreaseDesired;
    }

    @Override
    public int hashCode() {
        int result = (disableIncreaseDesired ? 1 : 0);
        result = 31 * result + (disableDecreaseDesired ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ServiceJobProcesses{" +
                "disableIncreaseDesired=" + disableIncreaseDesired +
                ", disableDecreaseDesired=" + disableDecreaseDesired +
                '}';
    }

    public static ServiceJobProcesses.Builder newBuilder() {
        return new ServiceJobProcesses.Builder();
    }


    public static final class Builder {
        private boolean disableIncreaseDesired;
        private boolean disableDecreaseDesired;

        private Builder() {
        }

        public Builder withDisableIncreaseDesired(boolean disableIncreaseDesired) {
            this.disableIncreaseDesired = disableIncreaseDesired;
            return this;
        }

        public Builder withDisableDecreaseDesired(boolean disableDecreaseDesired) {
            this.disableDecreaseDesired = disableDecreaseDesired;
            return this;
        }

        public ServiceJobProcesses build() {
            return new ServiceJobProcesses(disableIncreaseDesired, disableDecreaseDesired);
        }
    }
}
