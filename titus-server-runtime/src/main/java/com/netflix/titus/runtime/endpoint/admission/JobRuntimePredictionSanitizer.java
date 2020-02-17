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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.FunctionExt;
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
    private final JobRuntimePredictionConfiguration configuration;

    private final JobRuntimePredictionSanitizerMetrics metrics;

    @Inject
    public JobRuntimePredictionSanitizer(JobRuntimePredictionClient predictionsClient,
                                         JobRuntimePredictionSelector selector,
                                         JobRuntimePredictionConfiguration configuration,
                                         TitusRuntime runtime) {
        this.predictionsClient = predictionsClient;
        this.selector = selector;
        this.configuration = configuration;
        this.metrics = new JobRuntimePredictionSanitizerMetrics(runtime.getRegistry());
    }

    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor entity) {
        if (!JobFunctions.isBatchJob(entity)) {
            return Mono.empty();
        }
        if (isSkippedForJob(entity)) {
            metrics.jobOptedOut();
            return Mono.just(JobRuntimePredictionSanitizer::skipSanitization);
        }

        return predictionsClient.getRuntimePredictions(entity)
                .map(this::addPredictionToJob)
                .doOnError(throwable -> {
                    metrics.predictionServiceError();
                    logger.error("Error calling the job runtime prediction service, skipping prediction for {}: {}",
                            entity.getApplicationName(), throwable.getMessage());
                })
                .onErrorReturn(JobRuntimePredictionSanitizer::skipSanitization)
                // the runtime limit acts as a cap on the prediction, or a fallback when no prediction is selected
                .map(sanitizer -> FunctionExt.andThen(sanitizer, this::capPredictionToRuntimeLimit))
                .defaultIfEmpty(this::capPredictionToRuntimeLimit);
    }

    @SuppressWarnings("unchecked")
    private UnaryOperator<JobDescriptor> addPredictionToJob(JobRuntimePredictions predictions) {
        return jobDescriptor -> {
            Map<String, String> metadata = new HashMap<>();
            metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, predictions.getModelId());
            metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, predictions.getVersion());
            predictions.toSimpleString().ifPresent(predictionsStr ->
                    metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, predictionsStr)
            );

            Optional<JobRuntimePredictionSelection> selectionOpt;
            if (isValid(predictions)) {
                selectionOpt = FunctionExt.ifNotPresent(
                        selector.apply(jobDescriptor, predictions),
                        metrics::noPredictionSelected
                );
            } else {
                selectionOpt = Optional.empty();
                metrics.invalidPredictions();
            }

            // extra local variable otherwise compiler type inference breaks
            Optional<JobDescriptor> resultOpt = selectionOpt
                    .map(selection -> {
                        metadata.putAll(selection.getMetadata());
                        metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, Double.toString(selection.getPrediction().getRuntimeInSeconds()));
                        metadata.put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, Double.toString(selection.getPrediction().getConfidence()));
                        return appendJobDescriptorAttributes(jobDescriptor, metadata);
                    });
            return resultOpt.orElseGet(() -> skipSanitization(appendJobDescriptorAttributes(jobDescriptor, metadata)));
        };
    }

    /**
     * Use the prediction when available and shorter than the runtime limit, otherwise the runtime limit becomes
     * the prediction if within {@link JobRuntimePredictionConfiguration#getMaxOpportunisticRuntimeLimitMs()}
     */
    @SuppressWarnings("unchecked")
    private JobDescriptor capPredictionToRuntimeLimit(JobDescriptor jobDescriptor) {
        // non-batch jobs have been filtered before this point, it is safe to cast
        BatchJobExt extensions = ((JobDescriptor<BatchJobExt>) jobDescriptor).getExtensions();
        long runtimeLimitMs = extensions.getRuntimeLimitMs();
        if (runtimeLimitMs <= 0 || runtimeLimitMs > configuration.getMaxOpportunisticRuntimeLimitMs()) {
            return jobDescriptor; // no runtime limit or too high to be used, noop
        }

        return JobFunctions.getJobRuntimePrediction(jobDescriptor)
                .filter(prediction -> runtimeLimitMs > prediction.toMillis())
                .map(ignored -> jobDescriptor)
                .orElseGet(() -> JobFunctions.appendJobDescriptorAttributes(jobDescriptor, ImmutableMap.<String, String>builder()
                        .put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, Double.toString(runtimeLimitMs / 1000.0))
                        .put(JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, Double.toString(1.0))
                        .build()));
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
