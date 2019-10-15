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

import java.util.Arrays;
import java.util.TreeSet;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePredictionClient;
import com.netflix.titus.runtime.connector.prediction.JobRuntimePredictions;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_PARAMETER_SKIP_RUNTIME_PREDICTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobRuntimePredictionSanitizerTest {

    private JobDescriptor<BatchJobExt> jobDescriptor;
    private String modelId;
    private JobRuntimePredictionClient client;

    @Before
    public void setup() {
        jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
        modelId = UUID.randomUUID().toString();
        client = mock(JobRuntimePredictionClient.class);

        JobRuntimePredictions predictions = new JobRuntimePredictions("v1", modelId,
                new TreeSet<>(Arrays.asList(
                        new JobRuntimePrediction(0.05, 5.5),
                        new JobRuntimePrediction(0.25, 6.0),
                        new JobRuntimePrediction(0.90, 20.1)
                )));
        when(client.getRuntimePredictions(jobDescriptor)).thenReturn(Mono.just(predictions));
    }

    @Test
    public void predictionsAreAddedToJobs() {
        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, "20.1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, "0.9")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, modelId)
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, "v1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, "0.05=5.5;0.25=6.0;0.9=20.1")
                )
                .verifyComplete();
    }

    @Test
    public void errorsCauseSanitizationToBeSkipped() {
        JobRuntimePredictionClient errorClient = mock(JobRuntimePredictionClient.class);
        when(errorClient.getRuntimePredictions(any())).thenReturn(Mono.error(TitusServiceException.internal("bad request")));

        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(errorClient,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, "true")
                        .doesNotContainKeys(
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION
                        )
                )
                .verifyComplete();
    }

    @Test
    public void emptyPredictionsCauseSanitizationToBeSkipped() {
        JobRuntimePredictionClient client = mock(JobRuntimePredictionClient.class);
        JobRuntimePredictions predictions = new JobRuntimePredictions("v1", modelId, new TreeSet<>());
        when(client.getRuntimePredictions(any())).thenReturn(Mono.just(predictions));

        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, "true")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, modelId)
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, "v1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, "")
                )
                .verifyComplete();
    }

    @Test
    public void zeroedPredictionsCauseSanitizationToBeSkipped() {
        JobRuntimePredictionClient client = mock(JobRuntimePredictionClient.class);
        JobRuntimePredictions predictions = new JobRuntimePredictions("v1", modelId,
                new TreeSet<>(Arrays.asList(
                        new JobRuntimePrediction(0.05, 0.0),
                        new JobRuntimePrediction(0.25, 6.0),
                        new JobRuntimePrediction(0.90, 21.2)
                )));
        when(client.getRuntimePredictions(any())).thenReturn(Mono.just(predictions));

        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, "true")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, modelId)
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, "v1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, "0.05=0.0;0.25=6.0;0.9=21.2")
                )
                .verifyComplete();
    }

    @Test
    public void higherQualityPredictionsMustBeLonger() {
        JobRuntimePredictionClient client = mock(JobRuntimePredictionClient.class);
        JobRuntimePredictions predictions = new JobRuntimePredictions("v1", modelId,
                new TreeSet<>(Arrays.asList(
                        new JobRuntimePrediction(0.05, 5.1),
                        new JobRuntimePrediction(0.25, 4.0),
                        new JobRuntimePrediction(0.35, 4.1),
                        new JobRuntimePrediction(0.90, 4.0)
                )));
        when(client.getRuntimePredictions(any())).thenReturn(Mono.just(predictions));

        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, "true")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID, modelId)
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION, "v1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, "0.05=5.1;0.25=4.0;0.35=4.1;0.9=4.0")
                )
                .verifyComplete();
    }

    @Test
    public void metadataFromSelectorIsAddedToJobs() {
        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(CollectionsExt.asMap(
                        "selection.extra", "foobar",
                        // can't override prediction attributes
                        JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, "0.82",
                        JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, "1.0"
                )), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(jobDescriptor))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, "20.1")
                        .containsEntry(JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE, "0.9")
                        .containsEntry("selection.extra", "foobar")
                )
                .verifyComplete();
    }

    @Test
    public void jobsCanOptOut() {
        JobDescriptor<BatchJobExt> optedOut = jobDescriptor.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jobDescriptor.getAttributes(),
                        JOB_PARAMETER_SKIP_RUNTIME_PREDICTION, "true"
                ))
                .build();
        AdmissionSanitizer<JobDescriptor> sanitizer = new JobRuntimePredictionSanitizer(client,
                JobRuntimePredictionSelectors.best(), TitusRuntimes.test());

        StepVerifier.create(sanitizer.sanitizeAndApply(optedOut))
                .assertNext(result -> assertThat(((JobDescriptor<?>) result).getAttributes())
                        .containsEntry(JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION, "true")
                        .doesNotContainKeys(
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID,
                                JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION
                        )
                )
                .verifyComplete();
    }
}