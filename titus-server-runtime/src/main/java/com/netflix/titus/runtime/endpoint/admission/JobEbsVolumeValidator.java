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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.common.model.admission.AdmissionValidator;
import com.netflix.titus.common.model.admission.ValidatorMetrics;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import reactor.core.publisher.Mono;

/**
 * This {@link com.netflix.titus.common.model.admission.AdmissionValidator} validates Job EBS volumes
 * have all of the appropriate volume metadata set. This metadata should be set during a prior sanitization step.
 */
public class JobEbsVolumeValidator implements AdmissionValidator<JobDescriptor> {

    private static final String REASON_MISSING_FIELD = "missingField";

    private final Supplier<ValidationError.Type> validationErrorTypeProvider;
    private final ValidatorMetrics metrics;

    public JobEbsVolumeValidator(Supplier<ValidationError.Type> validationErrorTypeProvider,
                                 TitusRuntime titusRuntime) {
        this.validationErrorTypeProvider = validationErrorTypeProvider;
        this.metrics = new ValidatorMetrics(JobEbsVolumeValidator.class.getSimpleName(), titusRuntime.getRegistry());
    }

    @Override
    public Mono<Set<ValidationError>> validate(JobDescriptor jobDescriptor) {
        return Mono.fromCallable(() -> CollectionsExt.merge(
                validateFieldsSet(jobDescriptor),
                validateDuplicateVolumeIds(jobDescriptor)
        ))
                .doOnNext(validationErrors -> {
                    if (validationErrors.isEmpty()) {
                        metrics.incrementValidationSuccess(JobAttributes.JOB_ATTRIBUTES_EBS_VOLUME_IDS);
                    }
                });
    }

    @Override
    public ValidationError.Type getErrorType() {
        return validationErrorTypeProvider.get();
    }

    /**
     * Validates that all required EBS fields are set.
     */
    private Set<ValidationError> validateFieldsSet(JobDescriptor jobDescriptor) {
        return jobDescriptor.getContainer().getContainerResources().getEbsVolumes().stream()
                .filter(ebsVolume -> StringExt.isEmpty(ebsVolume.getVolumeAvailabilityZone()) ||
                        ebsVolume.getVolumeCapacityGB() == 0)
                .peek(ebsVolume -> metrics.incrementValidationError(ebsVolume.getVolumeId(), REASON_MISSING_FIELD))
                .map(ebsVolume -> new ValidationError(
                        JobAttributes.JOB_ATTRIBUTES_EBS_VOLUME_IDS,
                        String.format("Required field missing from EBS volume %s", ebsVolume)))
                .collect(Collectors.toSet());
    }

    /**
     * Validates that there are no duplicate volume IDs.
     */
    private Set<ValidationError> validateDuplicateVolumeIds(JobDescriptor jobDescriptor) {
        return jobDescriptor.getContainer().getContainerResources().getEbsVolumes().stream()
                .map(EbsVolume::getVolumeId)
                .distinct()
                .count() == jobDescriptor.getContainer().getContainerResources().getEbsVolumes().size()
                ? Collections.emptySet()
                : Collections.singleton(new ValidationError(
                JobAttributes.JOB_ATTRIBUTES_EBS_VOLUME_IDS,
                "Duplicate volume IDs exist"));
    }
}
