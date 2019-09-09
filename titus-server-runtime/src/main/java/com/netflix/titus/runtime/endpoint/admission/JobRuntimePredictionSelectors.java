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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.SortedSet;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;

public final class JobRuntimePredictionSelectors {

    private static final JobRuntimePredictionSelector NEVER_SELECTOR = (jobDescriptor, predictions) -> Optional.empty();

    private JobRuntimePredictionSelectors() {
    }

    public static JobRuntimePredictionSelector never() {
        return NEVER_SELECTOR;
    }

    /**
     * Always select the highest quality prediction.
     */
    public static JobRuntimePredictionSelector best() {
        return best(Collections.emptyMap());
    }

    /**
     * Always select the highest quality prediction, with additional metadata.
     */
    public static JobRuntimePredictionSelector best(Map<String, String> selectionMetadata) {
        return (jobDescriptor, predictions) -> CollectionsExt.isNullOrEmpty(predictions.getPredictions())
                ? Optional.empty()
                : Optional.of(JobRuntimePredictionSelection.newBuilder().withPrediction(predictions.getPredictions().last()).withMetadata(selectionMetadata).build());
    }

    /**
     * Select prediction with the lowest confidence level which is above the provided confidenceThreshold.
     */
    public static JobRuntimePredictionSelector aboveThreshold(double confidenceThreshold, Map<String, String> selectionMetadata) {
        return (jobDescriptor, predictions) -> {
            SortedSet<JobRuntimePrediction> predictionsSet = predictions.getPredictions();
            if (CollectionsExt.isNullOrEmpty(predictionsSet)) {
                return Optional.empty();
            }
            for (JobRuntimePrediction prediction : predictionsSet) {
                if (prediction.getConfidence() > confidenceThreshold) {
                    return Optional.of(JobRuntimePredictionSelection.newBuilder().withPrediction(prediction).withMetadata(selectionMetadata).build());
                }
            }
            return Optional.empty();
        };
    }

    /**
     * AB test choosing randomly from the provided collection of selectors. At each invocation, a one
     * downstream selector is chosen, and the result is tagged with its id.
     */
    public static JobRuntimePredictionSelector abTestRandom(Map<String, JobRuntimePredictionSelector> selectors) {
        if (CollectionsExt.isNullOrEmpty(selectors)) {
            return never();
        }
        List<Map.Entry<String, JobRuntimePredictionSelector>> entries = new ArrayList<>(selectors.entrySet());
        Random random = new Random(System.nanoTime());

        return (jobDescriptor, predictions) -> {

            Map.Entry<String, JobRuntimePredictionSelector> entry = entries.get(random.nextInt(selectors.size()));
            String id = entry.getKey();
            JobRuntimePredictionSelector selector = entry.getValue();

            return selector.apply(jobDescriptor, predictions).map(selection -> {
                Map<String, String> metadata = CollectionsExt.copyAndAdd(selection.getMetadata(), JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AB_TEST, id);
                return selection.toBuilder().withMetadata(metadata).build();
            });
        };
    }
}
