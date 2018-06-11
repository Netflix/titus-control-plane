/*
 * Copyright 2018 Netflix, Inc.
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

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtendedJobSanitizerTest {

    private static final int MIN_DISK_SIZE = 10_000;
    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final EntitySanitizer entitySanitizer = mock(EntitySanitizer.class);

    @Before
    public void setUp() {
        when(configuration.getNoncompliantClientWhiteList()).thenReturn("_none_");
    }

    @Test
    public void testDiskSizeIsChangedToMin() {
        int diskSize = 100;
        JobDescriptor jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().but(c -> c.getContainerResources().toBuilder().withDiskMB(diskSize))))
                .cast(JobDescriptor.class).getValue();

        when(configuration.getMinDiskSize()).thenReturn(MIN_DISK_SIZE);
        when(entitySanitizer.sanitize(any())).thenReturn(Optional.of(jobDescriptor));

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, entitySanitizer);
        Optional<JobDescriptor> sanitizedJobDescriptorOpt = sanitizer.sanitize(jobDescriptor);
        JobDescriptor sanitizedJobDescriptor = sanitizedJobDescriptorOpt.get();
        assertThat(sanitizedJobDescriptor).isNotNull();
        assertThat(sanitizedJobDescriptor.getContainer().getContainerResources().getDiskMB()).isEqualTo(MIN_DISK_SIZE);
        String nonCompliant = (String) sanitizedJobDescriptor.getAttributes().get("titus.noncompliant");
        assertThat(nonCompliant).contains("diskSizeLessThanMin");
    }

    @Test
    public void testDiskSizeIsNotChanged() {
        int diskSize = 11_000;
        JobDescriptor jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().but(c -> c.getContainerResources().toBuilder().withDiskMB(diskSize))))
                .cast(JobDescriptor.class).getValue();

        when(configuration.getMinDiskSize()).thenReturn(MIN_DISK_SIZE);
        when(entitySanitizer.sanitize(any())).thenReturn(Optional.of(jobDescriptor));

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, entitySanitizer);
        Optional<JobDescriptor> sanitizedJobDescriptorOpt = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitizedJobDescriptorOpt).isEmpty();
    }
}