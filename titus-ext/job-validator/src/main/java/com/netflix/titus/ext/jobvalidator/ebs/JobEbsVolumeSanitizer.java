/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.ext.jobvalidator.ebs;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.inject.Inject;

import com.netflix.compute.validator.protogen.ComputeValidator;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.model.admission.AdmissionSanitizer;
import com.netflix.titus.common.model.admission.ValidatorMetrics;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.jobvalidator.s3.ReactorValidationServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * This {@link com.netflix.titus.common.model.admission.AdmissionSanitizer} sanitizes Job EBS volume
 * information. Sanitization adds required EBS volume metadata (e.g., AZ, capacity, etc...) that are
 * retrieved from the validation service.
 */
public class JobEbsVolumeSanitizer implements AdmissionSanitizer<JobDescriptor> {

    private static final Logger logger = LoggerFactory.getLogger(JobEbsVolumeSanitizer.class);

    private static final long RETRY_COUNT = 5;

    private final JobEbsVolumeSanitizerConfiguration configuration;
    private final ReactorValidationServiceClient validationClient;
    private final ValidatorMetrics metrics;

    @Inject
    public JobEbsVolumeSanitizer(JobEbsVolumeSanitizerConfiguration configuration,
                                 ReactorValidationServiceClient validationClient,
                                 TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.validationClient = validationClient;
        this.metrics = new ValidatorMetrics(JobEbsVolumeSanitizer.class.getSimpleName(), titusRuntime.getRegistry());
    }

    /**
     * @return a {@link UnaryOperator} that adds sanitized EBS volume List.
     */
    @Override
    public Mono<UnaryOperator<JobDescriptor>> sanitize(JobDescriptor jobDescriptor) {
        if (!configuration.isEnabled()) {
            metrics.incrementValidationSkipped(ValidatorMetrics.REASON_DISABLED);
            return Mono.just(JobEbsVolumeSanitizer::skipSanitization);
        }

        List<EbsVolume> ebsVolumes = jobDescriptor.getContainer().getContainerResources().getEbsVolumes();
        return Flux.fromIterable(ebsVolumes)
                // Execute validation service calls concurrently
                .parallel()
                .runOn(Schedulers.parallel())
                // Validate the volume and update the core EBS volume object
                .flatMap(ebsVolume -> getEbsVolumeValidationResponse(ebsVolume)
                        .flatMap(response -> sanitizeEbsVolume(ebsVolume, response))
                        .doOnEach(response -> metrics.incrementValidationSuccess(ebsVolume.getVolumeId()))
                        .doOnError(throwable -> metrics.incrementValidationError(ebsVolume.getVolumeId(), throwable.getMessage())))
                .collectSortedList(Comparator.comparing(EbsVolume::getVolumeId))
                // Update the job with sanitized ebs volume list
                .map(JobEbsVolumeSanitizer::setEbsFunction)
                .timeout(Duration.ofMillis(configuration.getJobEbsSanitizationTimeoutMs()));
    }

    private Mono<ComputeValidator.EbsVolumeValidationResponse> getEbsVolumeValidationResponse(EbsVolume ebsVolume) {
        return validationClient.validateEbsVolume(
                ComputeValidator.EbsVolumeValidationRequest.newBuilder()
                        .setEbsVolumeId(ebsVolume.getVolumeId())
                        .build())
                .retry(RETRY_COUNT)
                .onErrorMap(error -> {
                    logger.warn("EBS volume validation failure: {}", error.getMessage());
                    logger.debug("Stack trace", error);
                    metrics.incrementValidationError(ebsVolume.getVolumeId(), error.getClass().getSimpleName());

                    return new IllegalArgumentException(String.format("EBS volume validation error: bucket=%s, error=%s",
                            ebsVolume.getVolumeId(),
                            error.getMessage()
                    ), error);
                });
    }

    private Mono<EbsVolume> sanitizeEbsVolume(EbsVolume ebsVolume, ComputeValidator.EbsVolumeValidationResponse response) {
        if (response.getResultCase() == ComputeValidator.EbsVolumeValidationResponse.ResultCase.FAILURES) {
            List<ComputeValidator.ValidationFailure> failures = response.getFailures().getFailuresList();
            if (!failures.isEmpty()) {
                return Mono.error(JobManagerException.invalidContainerResources(ebsVolume, failures.get(0).getErrorMessage()));
            }
            return Mono.error(JobManagerException.invalidContainerResources(ebsVolume, "No failures reported"));
        }

        return Mono.just(ebsVolume.toBuilder()
                .withVolumeAvailabilityZone(response.getSuccess().getEbsVolumeAvailabilityZone())
                .withVolumeCapacityGB(response.getSuccess().getEbsVolumeCapacityGB())
                .build());
    }

    private static UnaryOperator<JobDescriptor> setEbsFunction(List<EbsVolume> ebsVolumes) {
        return entity -> entity.toBuilder()
                .withContainer(entity.getContainer().toBuilder()
                        .withContainerResources(entity.getContainer().getContainerResources().toBuilder()
                                .withEbsVolumes(ebsVolumes)
                                .build())
                        .build())
                .build();
    }

    private static JobDescriptor<?> skipSanitization(JobDescriptor<?> jobDescriptor) {
        return JobFunctions.appendJobDescriptorAttribute(jobDescriptor,
                JobAttributes.JOB_ATTRIBUTES_SANITIZATION_SKIPPED_EBS, true
        );
    }
}
