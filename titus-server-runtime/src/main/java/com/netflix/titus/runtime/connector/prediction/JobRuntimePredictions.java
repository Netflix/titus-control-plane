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

package com.netflix.titus.runtime.connector.prediction;

import java.util.Collection;
import java.util.SortedSet;

public class JobRuntimePredictions {
    private final String version;
    private final String modelId;
    private final SortedSet<JobRuntimePrediction> predictions;
    private final String simpleStringRepresentation;

    public JobRuntimePredictions(String version, String modelId, SortedSet<JobRuntimePrediction> predictions) {
        this.version = version;
        this.modelId = modelId;
        this.predictions = predictions;
        this.simpleStringRepresentation = buildSimpleString(predictions);
    }

    public String getVersion() {
        return version;
    }

    public String getModelId() {
        return modelId;
    }

    public SortedSet<JobRuntimePrediction> getPredictions() {
        return predictions;
    }

    /**
     * One line representation of all predictions in seconds, and their confidence percentile, e.g.:
     * <tt>0.1=10.0;0.2=15.1;0.9=40.5</tt>
     */
    public String toSimpleString() {
        return simpleStringRepresentation;
    }

    @Override
    public String toString() {
        return "JobRuntimePredictions{" +
                "version='" + version + '\'' +
                ", modelId='" + modelId + '\'' +
                ", predictions=" + predictions +
                '}';
    }

    private static String buildSimpleString(Collection<JobRuntimePrediction> predictions) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (JobRuntimePrediction prediction : predictions) {
            if (first) {
                first = false;
            } else {
                builder.append(';');
            }
            builder.append(prediction.getConfidence()).append('=').append(prediction.getRuntimeInSeconds());
        }
        return builder.toString();
    }
}
