/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.model.v2;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSla {

    public static class Builder {
        private static final ObjectMapper objectMapper;

        static {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        private int retries;
        private long runtimeLimit = 0L;
        private long minRuntimeSecs = 0L;
        private StreamSLAType slaType = StreamSLAType.Lossy;
        private V2JobDurationType durationType = V2JobDurationType.Perpetual;
        private Map<String, String> userProvidedTypes = new HashMap<>();

        public Builder withRetries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder withRuntimeLimit(long limit) {
            this.runtimeLimit = limit;
            return this;
        }

        public Builder withMinRuntimeSecs(long minRuntimeSecs) {
            this.minRuntimeSecs = minRuntimeSecs;
            return this;
        }

        public Builder withSlaType(StreamSLAType slaType) {
            this.slaType = slaType;
            return this;
        }

        public Builder withDurationType(V2JobDurationType durationType) {
            this.durationType = durationType;
            return this;
        }

        // Sets the job's unique tag value which is used to determine if two jobs are identical.
        // This is primarily used to determine if a job already exists and connect to it instead of submitting a
        // duplicate identical job.
        public Builder withUniqueJobTagValue(String value) {
            userProvidedTypes.put(uniqueTagName, value);
            return this;
        }

        public Builder withUserTag(String key, String value) {
            userProvidedTypes.put(key, value);
            return this;
        }

        public JobSla build() {
            try {
                return new JobSla(retries, runtimeLimit, minRuntimeSecs, slaType, durationType, objectMapper.writeValueAsString(userProvidedTypes));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unexpected error creating json out of user tags map: " + e.getMessage(), e);
            }
        }
    }

    public enum StreamSLAType {
        Lossy
    }

    public static final String uniqueTagName = "unique";

    private static final Logger logger = LoggerFactory.getLogger(JobSla.class);

    private final int retries;
    private final long runtimeLimitSecs;
    private final long minRuntimeSecs;
    private final StreamSLAType slaType;
    private final V2JobDurationType durationType;
    private final String userProvidedType;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobSla(@JsonProperty("retries") int retries,
                  @JsonProperty("runtimeLimitSecs") Long runtimeLimitSecs,
                  @JsonProperty("minRuntimeSecs") long minRuntimeSecs,
                  @JsonProperty("slaType") StreamSLAType slaType,
                  @JsonProperty("durationType") V2JobDurationType durationType,
                  @JsonProperty("userProvidedType") String userProvidedType) {
        this.retries = retries;
        this.runtimeLimitSecs = runtimeLimitSecs != null ? Math.max(0L, runtimeLimitSecs) : 0L;
        this.minRuntimeSecs = Math.max(0L, minRuntimeSecs);
        this.slaType = slaType == null ? StreamSLAType.Lossy : slaType;
        this.durationType = durationType;
        this.userProvidedType = userProvidedType;
    }

    public int getRetries() {
        return retries;
    }

    public long getRuntimeLimitSecs() {
        return runtimeLimitSecs;
    }

    public long getMinRuntimeSecs() {
        return minRuntimeSecs;
    }

    public StreamSLAType getSlaType() {
        return slaType;
    }

    public V2JobDurationType getDurationType() {
        return durationType;
    }

    public String getUserProvidedType() {
        return userProvidedType;
    }
}
