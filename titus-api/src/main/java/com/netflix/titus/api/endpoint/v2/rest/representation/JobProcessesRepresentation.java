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
package com.netflix.titus.api.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobProcessesRepresentation {
    private static Logger logger = LoggerFactory.getLogger(JobProcessesRepresentation.class);

    private final boolean disableIncreaseDesired;

    private final boolean disableDecreaseDesired;

    @JsonCreator
    public JobProcessesRepresentation(@JsonProperty("disableIncreaseDesired") boolean disableIncreaseDesired,
                                      @JsonProperty("disableDecreaseDesired") boolean disableDecreaseDesired) {
        this.disableIncreaseDesired = disableIncreaseDesired;
        this.disableDecreaseDesired = disableDecreaseDesired;
    }

    public boolean getDisableIncreaseDesired() {
        return disableIncreaseDesired;
    }

    public boolean getDisableDecreaseDesired() {
        return disableDecreaseDesired;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean disableIncreaseDesired;
        private boolean disableDecreaseDesired;

        private Builder() {

        }

        public Builder withDisabledIncreaseDesired(boolean disableIncreaseDesired) {
            this.disableIncreaseDesired = disableIncreaseDesired;
            return this;
        }

        public Builder withDisabledDecreaseDesired(boolean disableDecreaseDesired) {
            this.disableDecreaseDesired = disableDecreaseDesired;
            return this;
        }

        public JobProcessesRepresentation build() {
           return new JobProcessesRepresentation(this.disableIncreaseDesired, this.disableDecreaseDesired);
        }
    }

}
