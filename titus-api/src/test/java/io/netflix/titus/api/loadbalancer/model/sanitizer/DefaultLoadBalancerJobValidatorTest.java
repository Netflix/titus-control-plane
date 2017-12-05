/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.loadbalancer.model.sanitizer;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.model.job.Container;
import io.netflix.titus.api.jobmanager.model.job.ContainerResources;
import io.netflix.titus.api.jobmanager.model.job.Image;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerException;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultLoadBalancerJobValidatorTest {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerJobValidatorTest.class);

    private static final long TIMEOUT_MS = 30_000;

    private V3JobOperations jobOperations;
    private LoadBalancerStore loadBalancerStore;
    private LoadBalancerValidationConfiguration loadBalancerValidationConfiguration;
    private DefaultLoadBalancerJobValidator loadBalancerValidator;

    private static final String JOB_ID = "Titus-123";

    @Before
    public void setUp() throws Exception {
        jobOperations = mock(V3JobOperations.class);
        loadBalancerStore = new InMemoryLoadBalancerStore();
        loadBalancerValidationConfiguration = mock(LoadBalancerValidationConfiguration.class);

        loadBalancerValidator = new DefaultLoadBalancerJobValidator(jobOperations, loadBalancerStore, loadBalancerValidationConfiguration);
    }

    @Test
    public void testValidateJobExists() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.empty());
        Throwable thrown = catchThrowable(() -> loadBalancerValidator.validateJobId(JOB_ID));
        assertThat(thrown).isInstanceOf(JobManagerException.class);
        assertThat(((JobManagerException)thrown).getErrorCode()).isEqualTo(JobManagerException.ErrorCode.JobNotFound);
    }

    @Test
    public void testValidateJobAccepted() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.of(Job.newBuilder()
                .withId(JOB_ID)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Finished)
                        .build())
                .build()));
        Throwable thrown = catchThrowable(() -> loadBalancerValidator.validateJobId(JOB_ID));
        assertThat(thrown).isInstanceOf(JobManagerException.class);
        assertThat(((JobManagerException)thrown).getErrorCode()).isEqualTo(JobManagerException.ErrorCode.UnexpectedJobState);
    }

    @Test
    public void testValidateJobIsService() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.of(Job.<BatchJobExt>newBuilder()
                .withId(JOB_ID)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .build())
                .withJobDescriptor(JobDescriptor.<BatchJobExt>newBuilder()
                        .build())
                .build()));
        Throwable thrown = catchThrowable(() -> loadBalancerValidator.validateJobId(JOB_ID));
        assertThat(thrown).isInstanceOf(JobManagerException.class);
        assertThat(((JobManagerException)thrown).getErrorCode()).isEqualTo(JobManagerException.ErrorCode.NotServiceJob);
    }

    @Test
    public void testValidateJobAllocateIp() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.of(Job.<ServiceJobExt>newBuilder()
                .withId(JOB_ID)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .build())
                .withJobDescriptor(JobDescriptor.<ServiceJobExt>newBuilder()
                        .withExtensions(ServiceJobExt.newBuilder().build())
                        .withContainer(Container.newBuilder()
                                .withImage(Image.newBuilder().build())
                                .withContainerResources(ContainerResources.newBuilder()
                                        .withAllocateIP(false)
                                        .build())
                                .build())

                        .build())
                .build()));
        Throwable thrown = catchThrowable(() -> loadBalancerValidator.validateJobId(JOB_ID));
        assertThat(thrown).isInstanceOf(LoadBalancerException.class);
        assertThat(((LoadBalancerException)thrown).getErrorCode()).isEqualTo(LoadBalancerException.ErrorCode.JobNotRoutableIp);
    }

    @Test
    public void testValidateMaxLoadBalancers() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.of(Job.<ServiceJobExt>newBuilder()
                .withId(JOB_ID)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .build())
                .withJobDescriptor(JobDescriptor.<ServiceJobExt>newBuilder()
                        .withExtensions(ServiceJobExt.newBuilder().build())
                        .withContainer(Container.newBuilder()
                                .withImage(Image.newBuilder().build())
                                .withContainerResources(ContainerResources.newBuilder()
                                        .withAllocateIP(true)
                                        .build())
                                .build())
                        .build())
                .build()));

        when(loadBalancerValidationConfiguration.getMaxLoadBalancersPerJob()).thenReturn(30);

        for (int i = 0; i < loadBalancerValidationConfiguration.getMaxLoadBalancersPerJob() + 1; i++) {
            loadBalancerStore.addOrUpdateLoadBalancer(new JobLoadBalancer(JOB_ID, "LoadBalancer-" + i), JobLoadBalancer.State.Associated).await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }

        Throwable thrown = catchThrowable(() -> loadBalancerValidator.validateJobId(JOB_ID));
        assertThat(thrown).isInstanceOf(LoadBalancerException.class);
        assertThat(((LoadBalancerException)thrown).getErrorCode()).isEqualTo(LoadBalancerException.ErrorCode.JobMaxLoadBalancers);
    }

    @Test
    public void testValidJob() throws Exception {
        when(jobOperations.getJob(JOB_ID)).thenReturn(Optional.of(Job.<ServiceJobExt>newBuilder()
                .withId(JOB_ID)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .build())
                .withJobDescriptor(JobDescriptor.<ServiceJobExt>newBuilder()
                        .withExtensions(ServiceJobExt.newBuilder().build())
                        .withContainer(Container.newBuilder()
                                .withImage(Image.newBuilder().build())
                                .withContainerResources(ContainerResources.newBuilder()
                                        .withAllocateIP(true)
                                        .build())
                                .build())
                        .build())
                .build()));
        assertThatCode(() -> loadBalancerValidator.validateJobId(JOB_ID)).doesNotThrowAnyException();
    }
}
