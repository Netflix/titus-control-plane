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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Map;
import java.util.Objects;

import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;

public class JobRuntimePredictionSelection {

    private final JobRuntimePrediction prediction;
    private final Map<String, String> metadata;

    JobRuntimePredictionSelection(JobRuntimePrediction prediction, Map<String, String> metadata) {
        this.prediction = prediction;
        this.metadata = metadata;
    }

    public JobRuntimePrediction getPrediction() {
        return prediction;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobRuntimePredictionSelection that = (JobRuntimePredictionSelection) o;
        return Objects.equals(prediction, that.prediction) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prediction, metadata);
    }

    @Override
    public String toString() {
        return "JobRuntimePredictionSelection{" +
                "prediction=" + prediction +
                ", metadata=" + metadata +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withPrediction(prediction).withMetadata(metadata);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private JobRuntimePrediction prediction;
        private Map<String, String> metadata;

        private Builder() {
        }

        public Builder withPrediction(JobRuntimePrediction prediction) {
            this.prediction = prediction;
            return this;
        }

        public Builder withMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public JobRuntimePredictionSelection build() {
            return new JobRuntimePredictionSelection(prediction, metadata);
        }
    }
}
