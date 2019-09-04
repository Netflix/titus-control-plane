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

public class JobRuntimePrediction implements Comparable<JobRuntimePrediction> {
    private final double confidence;
    private final double runtimeInSeconds;

    public JobRuntimePrediction(double confidence, double runtime) {
        this.confidence = confidence;
        this.runtimeInSeconds = runtime;
    }

    /**
     * Statistical confidence associated with the runtimeInSeconds prediction, e.g.: 95% confidence will be 0.95
     */
    public double getConfidence() {
        return confidence;
    }

    /**
     * Predicted job runtime, in seconds
     */
    public double getRuntimeInSeconds() {
        return runtimeInSeconds;
    }

    /**
     * Predictions are ordered according to their quality (lower to higher):
     *
     * <ul>
     * <li>Higher confidence means higher quality</li>
     * <li>For the same confidence (a tie), a higher runtimeInSeconds prediction is safer and has higher quality</li>
     * </ul>
     */
    @Override
    public int compareTo(JobRuntimePrediction other) {
        int confidenceComparison = Double.compare(this.confidence, other.confidence);
        if (confidenceComparison != 0) {
            return confidenceComparison;
        }
        return Double.compare(this.runtimeInSeconds, other.runtimeInSeconds);
    }

    @Override
    public String toString() {
        return "JobRuntimePrediction{" +
                "confidence=" + confidence +
                ", runtimeInSeconds=" + runtimeInSeconds +
                '}';
    }
}
