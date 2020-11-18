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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.netflix.compute.validator.protogen.ComputeValidator;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.jobvalidator.s3.ReactorValidationServiceClient;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobEbsVolumeSanitizerTest {

    private static final EbsVolume EBS_VOLUME_A = EbsVolume.newBuilder()
            .withVolumeId("vol-a")
            .withMountPath("/a")
            .withMountPermissions(EbsVolume.MountPerm.RW)
            .withFsType("xfs")
            .build();
    private static final EbsVolume EBS_VOLUME_B = EbsVolume.newBuilder()
            .withVolumeId("vol-b")
            .withMountPath("/b")
            .withMountPermissions(EbsVolume.MountPerm.RW)
            .withFsType("xfs")
            .build();
    private static final List<EbsVolume> EBS_VOLUMES = Arrays.asList(EBS_VOLUME_A, EBS_VOLUME_B);
    private static final JobDescriptor<?> JOB_WITH_NO_EBS_VOLUMES = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
    private static final JobDescriptor<?> JOB_WITH_DEFAULT_MULTIPLE_EBS_VOLUMES = JOB_WITH_NO_EBS_VOLUMES.toBuilder()
            .withContainer(JOB_WITH_NO_EBS_VOLUMES.getContainer().toBuilder()
                    .withContainerResources(JOB_WITH_NO_EBS_VOLUMES.getContainer().getContainerResources().toBuilder()
                            .withEbsVolumes(EBS_VOLUMES)
                            .build())
                    .build())
            .build();

    private final ReactorValidationServiceClient validationClient = mock(ReactorValidationServiceClient.class);
    private final JobEbsVolumeSanitizerConfiguration configuration = mock(JobEbsVolumeSanitizerConfiguration.class);

    private final JobEbsVolumeSanitizer sanitizer = new JobEbsVolumeSanitizer(configuration, validationClient, TitusRuntimes.internal());

    @Before
    public void setUp() {
        when(configuration.isEnabled()).thenReturn(true);
        when(configuration.getJobEbsSanitizationTimeoutMs()).thenReturn(5_000L);
    }

    /**
     * Tests that EBS volume metadata returned by the validator service gets set properly
     * on the sanitized job.
     */
    @Test
    public void testMetadataAdded() {
        ComputeValidator.EbsVolumeValidationRequest request_a = ComputeValidator.EbsVolumeValidationRequest.newBuilder()
                .setEbsVolumeId(EBS_VOLUME_A.getVolumeId())
                .build();
        String azA = "us-east-1a";
        int sizeA = 5;
        EbsVolume sanitizedEbsVolumeA = EBS_VOLUME_A.toBuilder()
                .withVolumeAvailabilityZone(azA)
                .withVolumeCapacityGB(sizeA)
                .build();
        ComputeValidator.EbsVolumeValidationResponse response_a = ComputeValidator.EbsVolumeValidationResponse.newBuilder()
                .setSuccess(ComputeValidator.EbsVolumeValidationResponse.Success.newBuilder()
                        .setEbsVolumeAvailabilityZone(azA)
                        .setEbsVolumeCapacityGB(sizeA)
                        .build())
                .build();
        ComputeValidator.EbsVolumeValidationRequest request_b = ComputeValidator.EbsVolumeValidationRequest.newBuilder()
                .setEbsVolumeId(EBS_VOLUME_B.getVolumeId())
                .build();
        String azB = "us-east-1b";
        int sizeB = 10;
        EbsVolume sanitizedEbsVolumeB = EBS_VOLUME_B.toBuilder()
                .withVolumeAvailabilityZone(azB)
                .withVolumeCapacityGB(sizeB)
                .build();
        ComputeValidator.EbsVolumeValidationResponse response_b = ComputeValidator.EbsVolumeValidationResponse.newBuilder()
                .setSuccess(ComputeValidator.EbsVolumeValidationResponse.Success.newBuilder()
                        .setEbsVolumeAvailabilityZone(azB)
                        .setEbsVolumeCapacityGB(sizeB)
                        .build())
                .build();

        when(validationClient.validateEbsVolume(request_a)).thenReturn(Mono.just(response_a));
        when(validationClient.validateEbsVolume(request_b)).thenReturn(Mono.just(response_b));

        StepVerifier.create(sanitizer.sanitize(JOB_WITH_DEFAULT_MULTIPLE_EBS_VOLUMES))
                .expectNextMatches(operator -> jobContainsVolumes(
                        operator.apply(JOB_WITH_DEFAULT_MULTIPLE_EBS_VOLUMES),
                        CollectionsExt.asSet(sanitizedEbsVolumeA, sanitizedEbsVolumeB)))
                .verifyComplete();
    }

    /**
     * Test that a job with no EBS volumes is properly handled.
     */
    @Test
    public void testNoVolumes() {
        StepVerifier.create(sanitizer.sanitize(JOB_WITH_NO_EBS_VOLUMES))
                .expectNextMatches(operator -> operator.apply(JOB_WITH_NO_EBS_VOLUMES)
                        .getContainer().getContainerResources().getEbsVolumes().isEmpty())
                .verifyComplete();
    }

    /**
     * Tests that validation service error responses are handled properly.
     */
    @Test
    public void testValidatorError() {
        when(validationClient.validateEbsVolume(any())).thenReturn(Mono.error(new StatusRuntimeException(Status.INTERNAL)));
        StepVerifier.create(sanitizer.sanitize(JOB_WITH_DEFAULT_MULTIPLE_EBS_VOLUMES))
                .expectErrorMatches(throwable -> throwable instanceof IllegalArgumentException &&
                        throwable.getMessage().contains("EBS volume validation error"))
                .verify();
    }

    /**
     * Tests that validations that are unsuccessful/failures are handled properly.
     */
    @Test
    public void testValidationFailure() {
        ComputeValidator.EbsVolumeValidationResponse response = ComputeValidator.EbsVolumeValidationResponse.newBuilder()
                .setFailures(ComputeValidator.ValidationFailures.newBuilder()
                        .addFailures(ComputeValidator.ValidationFailure.newBuilder()
                                .setErrorCode("notFound")
                                .setErrorMessage("Volume not found")
                                .build())
                        .build())
                .build();

        when(validationClient.validateEbsVolume(any())).thenReturn(Mono.just(response));
        StepVerifier.create(sanitizer.sanitize(JOB_WITH_DEFAULT_MULTIPLE_EBS_VOLUMES))
                .expectErrorMatches(throwable -> throwable instanceof JobManagerException &&
                        throwable.getMessage().contains("Job has invalid EBS volume"))
                .verify();
    }

    private boolean jobContainsVolumes(JobDescriptor<?> jobDescriptor, Set<EbsVolume> ebsVolumeSet) {
        return new HashSet<>(jobDescriptor.getContainer().getContainerResources().getEbsVolumes()).equals(ebsVolumeSet);
    }
}
