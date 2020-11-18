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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.endpoint.admission.JobEbsVolumeValidator;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;
import reactor.test.StepVerifier;

public class JobEbsVolumeValidatorTest {

    private static final EbsVolume EBS_VOLUME_VALID = EbsVolume.newBuilder()
            .withVolumeId("vol-valid")
            .withMountPath("/valid")
            .withMountPermissions(EbsVolume.MountPerm.RW)
            .withFsType("xfs")
            .withVolumeCapacityGB(5)
            .withVolumeAvailabilityZone("us-east-1c")
            .build();
    private static final EbsVolume EBS_VOLUME_INVALID = EbsVolume.newBuilder()
            .withVolumeId("vol-invalid")
            .withMountPath("/invalid")
            .withMountPermissions(EbsVolume.MountPerm.RW)
            .withFsType("xfs")
            .build();
    private static final List<EbsVolume> INVALID_EBS_VOLUMES = Arrays.asList(EBS_VOLUME_VALID, EBS_VOLUME_INVALID);
    private static final List<EbsVolume> VALID_EBS_VOLUMES = Collections.singletonList(EBS_VOLUME_VALID);

    private static final JobDescriptor<?> JOB_WITH_NO_EBS_VOLUMES = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
    private static final JobDescriptor<?> JOB_WITH_INVALID_EBS_VOLUMES = JOB_WITH_NO_EBS_VOLUMES.toBuilder()
            .withContainer(JOB_WITH_NO_EBS_VOLUMES.getContainer().toBuilder()
                    .withContainerResources(JOB_WITH_NO_EBS_VOLUMES.getContainer().getContainerResources().toBuilder()
                            .withEbsVolumes(INVALID_EBS_VOLUMES)
                            .build())
                    .build())
            .build();
    private static final JobDescriptor<?> JOB_WITH_VALID_EBS_VOLUMES = JOB_WITH_NO_EBS_VOLUMES.toBuilder()
            .withContainer(JOB_WITH_NO_EBS_VOLUMES.getContainer().toBuilder()
                    .withContainerResources(JOB_WITH_NO_EBS_VOLUMES.getContainer().getContainerResources().toBuilder()
                            .withEbsVolumes(VALID_EBS_VOLUMES)
                            .build())
                    .build())
            .build();

    private final JobEbsVolumeValidator jobEbsVolumeValidator = new JobEbsVolumeValidator(() -> ValidationError.Type.HARD, TitusRuntimes.internal());

    @Test
    public void testJobWithInvalidEbsVolume() {
        StepVerifier.create(jobEbsVolumeValidator.validate(JOB_WITH_INVALID_EBS_VOLUMES))
                .expectNextMatches(violations -> violations.size() == 1)
                .verifyComplete();
    }

    @Test
    public void testJobWithValidEbsVolume() {
        StepVerifier.create(jobEbsVolumeValidator.validate(JOB_WITH_VALID_EBS_VOLUMES))
                .expectNextMatches(Set::isEmpty)
                .verifyComplete();
    }

    @Test
    public void testJobWithNoEbsVolumes() {
        StepVerifier.create(jobEbsVolumeValidator.validate(JOB_WITH_NO_EBS_VOLUMES))
                .expectNextMatches(Set::isEmpty)
                .verifyComplete();
    }
}
