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

package com.netflix.titus.api.jobmanager.model.job;

import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobCompatibilityTest {
    @Test
    public void testIdenticalJobs() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobCompatibility compatibility = JobCompatibility.of(reference, reference.toBuilder().build());
        assertThat(compatibility.isCompatible()).isTrue();
    }

    @Test
    public void testDifferentOwners() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail("other+123@netflix.com").build())
                .build();
        JobCompatibility compatibility = JobCompatibility.of(reference, other);
        assertThat(compatibility.isCompatible()).isTrue();
    }

    @Test
    public void testDifferentApplicationName() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withApplicationName("otherApp")
                .build();
        JobCompatibility compatibility = JobCompatibility.of(reference, other);
        assertThat(compatibility.isCompatible()).isTrue();
    }

    @Test
    public void testServiceExtInformationIsIgnored() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withExtensions(ServiceJobExt.newBuilder()
                        .withCapacity(Capacity.newBuilder()
                                .withMin(0)
                                .withDesired(100)
                                .withMax(200)
                                .build())
                        .withMigrationPolicy(SelfManagedMigrationPolicy.newBuilder().build())
                        .withRetryPolicy(ExponentialBackoffRetryPolicy.newBuilder().build())
                        .build())
                .build();

        JobCompatibility compatibility = JobCompatibility.of(reference, other);
        assertThat(compatibility.isCompatible()).isTrue();
    }

    @Test
    public void testDisruptionBudgetIsIgnored() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withDisruptionBudget(DisruptionBudget.newBuilder()
                        .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                                .withRelocationTimeMs(100)
                                .build())
                        .build())
                .build();

        JobCompatibility compatibility = JobCompatibility.of(reference, other);
        assertThat(compatibility.isCompatible()).isTrue();
    }

    @Test
    public void testAttributesNotPrefixedWithTitusAreCompatible() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(reference.getAttributes(), "spinnaker.useApplicationDefaultSecurityGroup", "true"))
                .build();
        JobCompatibility compatibility1 = JobCompatibility.of(reference, other);
        assertThat(compatibility1.isCompatible()).isTrue();

        JobDescriptor<ServiceJobExt> incompatible = reference.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(reference.getAttributes(), "titus.value", "important"))
                .build();
        JobCompatibility compatibility2 = JobCompatibility.of(reference, incompatible);
        assertThat(compatibility2.isCompatible()).isFalse();
    }

    @Test
    public void testContainerAttributesNotPrefixedWithTitusAreCompatible() {
        JobDescriptor<ServiceJobExt> reference = JobDescriptorGenerator.oneTaskServiceJobDescriptor();
        JobDescriptor<ServiceJobExt> other = reference.toBuilder()
                .withContainer(reference.getContainer().but(container ->
                        container.toBuilder()
                                .withAttributes(CollectionsExt.copyAndAdd(container.getAttributes(), "NETFLIX_APP_METADATA_SIG", "some signature"))
                                .build()
                ))
                .build();
        JobCompatibility compatibility1 = JobCompatibility.of(reference, other);
        assertThat(compatibility1.isCompatible()).isTrue();

        JobDescriptor<ServiceJobExt> incompatible = reference.toBuilder()
                .withContainer(reference.getContainer().but(container ->
                        container.toBuilder()
                                .withAttributes(CollectionsExt.copyAndAdd(container.getAttributes(), "titusParameter.cpu.burstEnabled", "true"))
                                .build()
                ))
                .build();
        JobCompatibility compatibility2 = JobCompatibility.of(reference, incompatible);
        assertThat(compatibility2.isCompatible()).isFalse();
    }
}
