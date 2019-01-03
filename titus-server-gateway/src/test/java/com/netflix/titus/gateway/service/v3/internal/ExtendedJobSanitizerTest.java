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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.FeatureRolloutPlans;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.gateway.service.v3.JobManagerConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.api.FeatureRolloutPlans.ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE;
import static com.netflix.titus.gateway.service.v3.internal.ExtendedJobSanitizer.TITUS_NON_COMPLIANT_FEATURES;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtendedJobSanitizerTest {

    private static final int MIN_DISK_SIZE = 10_000;

    private static final List<String> DEFAULT_SECURITY_GROUPS = asList("sg-1", "sg-2");
    private static final String DEFAULT_IAM_ROLE = "defaultIamRole";

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final JobConfiguration jobConfiguration = mock(JobConfiguration.class);
    private final EntitySanitizer entitySanitizer = mock(EntitySanitizer.class);
    private final JobAssertions jobAssertions = new JobAssertions(jobConfiguration, instance -> ResourceDimension.empty());

    @Before
    public void setUp() {
        when(configuration.getNoncompliantClientWhiteList()).thenReturn("_none_");
    }

    @Test
    public void testSecurityGroupsAndNoValidationFailures() {
        testSecurityGrupValidation(false, DEFAULT_SECURITY_GROUPS);
    }

    @Test
    public void testSecurityGroupsWithValidationFailures() {
        testSecurityGrupValidation(true, Collections.emptyList());
    }

    private void testSecurityGrupValidation(boolean doNotAddIfMissing, List<String> expected) {
        JobDescriptor jobDescriptor = newJobDescriptorWithSecurityProfile(Collections.emptyList(), "myIamRole");
        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> doNotAddIfMissing, jd -> false, titusRuntime);

        when(configuration.getDefaultSecurityGroups()).thenReturn(asList("sg-1", "sg-2"));

        Optional<JobDescriptor> sanitized = sanitizer.sanitize(jobDescriptor);

        assertThat(sanitized).isPresent();
        assertThat(sanitized.get().getContainer().getSecurityProfile().getSecurityGroups()).isEqualTo(expected);
    }

    @Test
    public void testIamRoleAndNoValidationFailures() {
        testIamRoleValidation(false, DEFAULT_IAM_ROLE);
    }

    @Test
    public void testIamRoleWithValidationFailures() {
        testIamRoleValidation(true, "");
    }

    private void testIamRoleValidation(boolean doNotAddIfMissing, String expected) {
        JobDescriptor jobDescriptor = newJobDescriptorWithSecurityProfile(DEFAULT_SECURITY_GROUPS, "");
        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> doNotAddIfMissing, jd -> false, titusRuntime);

        when(configuration.getDefaultIamRole()).thenReturn(DEFAULT_IAM_ROLE);

        Optional<JobDescriptor> sanitized = sanitizer.sanitize(jobDescriptor);

        assertThat(sanitized).isPresent();
        assertThat(sanitized.get().getContainer().getSecurityProfile().getIamRole()).isEqualTo(expected);
    }

    @Test
    public void testDiskSizeIsChangedToMin() {
        JobDescriptor jobDescriptor = newJobDescriptorWithDiskSize(100);

        when(configuration.getMinDiskSizeMB()).thenReturn(MIN_DISK_SIZE);
        when(entitySanitizer.sanitize(any())).thenReturn(Optional.of(jobDescriptor));

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);
        Optional<JobDescriptor> sanitizedJobDescriptorOpt = sanitizer.sanitize(jobDescriptor);
        JobDescriptor sanitizedJobDescriptor = sanitizedJobDescriptorOpt.get();
        assertThat(sanitizedJobDescriptor).isNotNull();
        assertThat(sanitizedJobDescriptor.getContainer().getContainerResources().getDiskMB()).isEqualTo(MIN_DISK_SIZE);
        String nonCompliant = (String) sanitizedJobDescriptor.getAttributes().get(TITUS_NON_COMPLIANT_FEATURES);
        assertThat(nonCompliant).contains(FeatureRolloutPlans.MIN_DISK_SIZE_STRICT_VALIDATION_FEATURE);
    }

    @Test
    public void testDiskSizeIsNotChanged() {
        JobDescriptor jobDescriptor = newJobDescriptorWithDiskSize(11_000);

        when(configuration.getMinDiskSizeMB()).thenReturn(MIN_DISK_SIZE);
        when(entitySanitizer.sanitize(any())).thenReturn(Optional.of(jobDescriptor));

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);
        Optional<JobDescriptor> sanitizedJobDescriptorOpt = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitizedJobDescriptorOpt).isEmpty();
    }

    @Test
    public void testFlatStringEntryPoint() {
        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withEntryPoint(Collections.singletonList("/bin/sh -c \"sleep 10\""))))
                .getValue();

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);
        Optional<JobDescriptor<?>> sanitized = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitized).isPresent();
        Map<String, String> attributes = sanitized.get().getAttributes();
        assertThat(attributes).containsKey(TITUS_NON_COMPLIANT_FEATURES);
        List<String> problems = asList(attributes.get(TITUS_NON_COMPLIANT_FEATURES).split(","));
        assertThat(problems).contains(FeatureRolloutPlans.ENTRY_POINT_STRICT_VALIDATION_FEATURE);
    }

    @Test
    public void testValidEntryPoint() {
        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withEntryPoint(asList("/bin/sh", "-c", "sleep 10"))))
                .getValue();

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);
        Optional<JobDescriptor<?>> sanitized = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitized).isNotPresent();
    }

    @Test
    public void testJobsWithCommandAreNotMarkedNonCompliant() {
        // ... because they never relied on shell parsing

        JobDescriptor<?> jobDescriptor = JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().toBuilder()
                        .withEntryPoint(Collections.singletonList("a binary with spaces"))
                        .withCommand(asList("some", "arguments"))))
                .getValue();

        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);
        Optional<JobDescriptor<?>> sanitized = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitized).isNotPresent();
    }

    @Test
    public void testEnvironmentNamesWithInvalidCharactersAndNoValidationFailures() {
        JobDescriptor jobDescriptor = newJobDescriptorWithEnvironment(";;;", "value");
        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> false, titusRuntime);

        Optional<JobDescriptor> sanitized = sanitizer.sanitize(jobDescriptor);
        assertThat(sanitized).isNotEmpty();
        assertThat(sanitized.get().getAttributes().get(TITUS_NON_COMPLIANT_FEATURES)).isEqualTo(ENVIRONMENT_VARIABLE_NAMES_STRICT_VALIDATION_FEATURE);
    }

    @Test(expected = TitusServiceException.class)
    public void testEnvironmentNamesWithInvalidCharactersAndWithValidationFailures() {
        JobDescriptor jobDescriptor = newJobDescriptorWithEnvironment(";;;", "value");
        ExtendedJobSanitizer sanitizer = new ExtendedJobSanitizer(configuration, jobAssertions, entitySanitizer, jd -> false, jd -> true, titusRuntime);

        sanitizer.sanitize(jobDescriptor);
    }

    private JobDescriptor newJobDescriptorWithSecurityProfile(List<String> securityGroups, String iamRole) {
        SecurityProfile securityProfile = SecurityProfile.newBuilder()
                .withIamRole(iamRole)
                .withSecurityGroups(securityGroups)
                .build();
        return JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().but(c -> c.toBuilder().withSecurityProfile(securityProfile).build())))
                .getValue();
    }

    private JobDescriptor newJobDescriptorWithEnvironment(String key, String value) {
        return JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().but(c -> c.toBuilder().withEnv(Collections.singletonMap(key, value)).build())))
                .getValue();
    }

    private JobDescriptor<BatchJobExt> newJobDescriptorWithDiskSize(int diskSize) {
        return JobDescriptorGenerator.batchJobDescriptors()
                .map(jd -> jd.but(d -> d.getContainer().but(c -> c.getContainerResources().toBuilder().withDiskMB(diskSize))))
                .getValue();
    }
}
