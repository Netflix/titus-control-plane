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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePredictionClient;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePredictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_SKIP_RUNTIME_PREDICTION;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.appendJobDescriptorAttributes;

/**
 * Decorates {@link JobDescriptor job descriptors} with a prediction of their expected runtime. All information about
 * predictions will be available as Job attributes.
 */
@Singleton
public class JobRuntimePredictionSanitizer implements AdmissionSanitizer<JobDescriptor> {
    private static final Logger logger = LoggerFactory.getLogger(JobRuntimePredictionSanitizer.class);

    private final JobRuntimePredictionClient predictionsClient;
    private final JobRuntimePredictionSelector selector;

    @Inject
    public JobRuntimePredictionSanitizer(JobRuntimePredictionClient predictionsClient, JobRuntimePredictionSelector selector) {
        this.predictionsClient = predictionsClient;
        this.selector = selector;
    }

    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor entity) {
        if (!JobFunctions.isBatchJob(entity)) {
            return Mono.empty();
        }
        if (isSkippedForJob(entity)) {
            return Mono.just(JobRuntimePredictionSanitizer::skipSanitization);
        }

        return predictionsClient.getRuntimePredictions(entity)
                .map(this::addPredictionToJob)
                .doOnError(throwable ->
                        logger.error("Error calling the job runtime prediction service, skipping prediction for {}: {}",
                                entity.getApplicationName(), throwable.getMessage())
                )
                .onErrorReturn(JobRuntimePredictionSanitizer::skipSanitization);
    }

    private UnaryOperator<JobDescriptor> addPredictionToJob(JobRuntimePredictions predictions) {
        Optional<JobRuntimePrediction> predictionOpt;
        Map<String, String> attributes;
        if (isValid(predictions)) {
            Pair<Optional<JobRuntimePrediction>, Map<String, String>> predictionWithAttributes = selector.apply(predictions);
            predictionOpt = predictionWithAttributes.getLeft();
            attributes = predictionWithAttributes.getRight();
        } else {
            predictionOpt = Optional.empty();
            attributes = Collections.emptyMap();
        }

        return jobDescriptor -> {
            Map<String, String> metadata = new HashMap<>(((JobDescriptor<?>) jobDescriptor).getAttributes());
            metadata.putAll(attributes);
            metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, predictions.getModelId());
            metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, predictions.getVersion());
            metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, predictions.toSimpleString());
            JobDescriptor<?> withPredictionMetadata = jobDescriptor.toBuilder().withAttributes(metadata).build();

            //noinspection unchecked
            return predictionOpt.map(prediction -> appendJobDescriptorAttributes(withPredictionMetadata, CollectionsExt.asMap(
                    JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, Double.toString(prediction.getRuntimeInSeconds()),
                    JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, Double.toString(prediction.getConfidence())
            ))).orElseGet(() -> skipSanitization(withPredictionMetadata));
        };
    }

    private static boolean isValid(JobRuntimePredictions predictions) {
        if (CollectionsExt.isNullOrEmpty(predictions.getPredictions())) {
            return false;
        }
        // higher quality predictions are always expected to be longer, and no zeroes allowed
        double lastSeen = 0.0;
        for (JobRuntimePrediction prediction : predictions.getPredictions()) {
            double current = prediction.getRuntimeInSeconds();
            if (current <= 0.0 || current < lastSeen) {
                return false;
            }
            lastSeen = current;
        }
        return true;
    }

    private static boolean isSkippedForJob(JobDescriptor<?> entity) {
        return Boolean.parseBoolean(
                entity.getAttributes().getOrDefault(JOB_PARAMETER_SKIP_RUNTIME_PREDICTION, "false").trim()
        );
    }

    @SuppressWarnings("unchecked")
    private static JobDescriptor skipSanitization(JobDescriptor jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, true
        );
    }
}
