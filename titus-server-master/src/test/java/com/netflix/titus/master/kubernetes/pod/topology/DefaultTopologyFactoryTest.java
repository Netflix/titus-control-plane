/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.topology;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.officeHourTimeWindow;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTopologyFactoryTest {

    private static final DisruptionBudget PERCENTAGE_OF_HEALTH_POLICY = budget(
            percentageOfHealthyPolicy(95), unlimitedRate(), Collections.singletonList(officeHourTimeWindow())
    );

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);
    private final FeatureActivationConfiguration features = mock(FeatureActivationConfiguration.class);

    private TopologyFactory topologyFactory;

    @Before
    public void setUp() throws Exception {
        topologyFactory = new DefaultTopologyFactory(configuration, features);
        when(configuration.getDisabledJobSpreadingPattern()).thenReturn("NONE");
        when(features.isRelocationBinpackingEnabled()).thenReturn(true);
    }

    @Test
    public void testHardConstraintNameIsCaseInsensitive() {
        testConstraintNameIsCaseInsensitive(JobFunctions.appendHardConstraint(JobGenerator.oneBatchJob(), "ZoneBalance", "true"));
    }

    @Test
    public void testSoftConstraintNameIsCaseInsensitive() {
        testConstraintNameIsCaseInsensitive(JobFunctions.appendSoftConstraint(JobGenerator.oneBatchJob(), "ZoneBalance", "true"));
    }

    @Test
    public void testBatchJobSpreading() {
        // By default no job spreading
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        List<V1TopologySpreadConstraint> constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).isEmpty();

        // Enable via job attribute
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED, "true");
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW, "10");
        constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getMaxSkew()).isEqualTo(10);

        // And now add zone constraint
        job = JobFunctions.appendSoftConstraint(job, JobConstraints.ZONE_BALANCE, "true");
        constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(2);
    }

    @Test
    public void testServiceJobSpreadingWithAvailabilityPercentageDisruptionBudget() {
        // By default no job spreading
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        job = JobFunctions.changeServiceJobCapacity(job, Capacity.newBuilder().withDesired(100).withMax(100).build());
        job = JobFunctions.changeDisruptionBudget(job, PERCENTAGE_OF_HEALTH_POLICY);
        List<V1TopologySpreadConstraint> constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getMaxSkew()).isEqualTo(5);

        // And now add zone constraint
        job = JobFunctions.appendSoftConstraint(job, JobConstraints.ZONE_BALANCE, "true");
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW, "10");
        constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(2);
        assertThat(constraints.get(1).getMaxSkew()).isEqualTo(10);

        // Disable via job attribute
        job = JobFunctions.appendJobDescriptorAttribute(job, JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED, "false");
        constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
    }

    @Test
    public void testJobSpreadingDisabledConfiguration() {
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        assertThat(topologyFactory.buildTopologySpreadConstraints(job)).hasSize(1);

        when(configuration.getDisabledJobSpreadingPattern()).thenReturn(".*");
        assertThat(topologyFactory.buildTopologySpreadConstraints(job)).isEmpty();
    }

    @Test
    public void jobSpreadingDisabledWhenBinpackingForRelocation() {
        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor().but(
                jd -> jd.getDisruptionBudget().toBuilder()
                        .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder().build())
        );
        Job<ServiceJobExt> job = JobGenerator.serviceJobs(jobDescriptor).getValue();
        assertThat(topologyFactory.buildTopologySpreadConstraints(job)).isEmpty();
    }

    private void testConstraintNameIsCaseInsensitive(Job<BatchJobExt> job) {
        List<V1TopologySpreadConstraint> constraints = topologyFactory.buildTopologySpreadConstraints(job);
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getTopologyKey()).isEqualTo(KubeConstants.NODE_LABEL_ZONE);
    }
}