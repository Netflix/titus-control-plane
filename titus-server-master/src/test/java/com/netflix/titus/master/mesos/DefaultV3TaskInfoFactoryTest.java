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

package com.netflix.titus.master.mesos;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.scheduler.constraint.V3ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultV3TaskInfoFactoryTest {

    private MasterConfiguration masterConfiguration;

    @Before
    public void setUp() throws Exception {
        masterConfiguration = mock(MasterConfiguration.class);
        when(masterConfiguration.pathToTitusExecutor()).thenReturn("/usr/bin/titus-executor");
    }

    @Test
    public void additionalPassthroughAttributes() throws InvalidProtocolBufferException {
        DefaultV3TaskInfoFactory factory = new DefaultV3TaskInfoFactory(masterConfiguration, mock(MesosConfiguration.class));
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
        Protos.TaskInfo taskInfo = buildTaskInfo(factory, jobDescriptor, Collections.singletonMap("extra", "attribute"), Optional.empty());
        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        assertThat(containerInfo.getPassthroughAttributesMap()).containsEntry("extra", "attribute");
    }

    @Test
    public void useRequestedCpusInsteadOfAssigned() throws InvalidProtocolBufferException {
        DefaultV3TaskInfoFactory factory = new DefaultV3TaskInfoFactory(masterConfiguration, mock(MesosConfiguration.class));
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor().but(jd -> jd.toBuilder()
                .withAttributes(CollectionsExt.copyAndAdd(jd.getAttributes(), CollectionsExt.asMap(
                        JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, "12.5",
                        JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE, "0.1=1;0.5=5.2;0.95=12.5"
                )))
        );
        double requestedCpus = jobDescriptor.getContainer().getContainerResources().getCpu();

        // opportunistic task with a runtime prediction
        Protos.TaskInfo taskInfo = buildTaskInfo(factory, jobDescriptor, Collections.emptyMap(),
                Optional.of(Duration.ofMinutes(1)));
        assertThat(taskInfo.getResourcesList()).anySatisfy(resource -> {
            assertThat(resource.getName()).isEqualTo("opportunisticCpus");
            assertThat(resource.getScalar().getValue()).isGreaterThan(0);
        });
        // CPUs must always be what was requested, regardless if some of them were allocated opportunistically
        assertThat(taskInfo.getResourcesList()).contains(
                Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(requestedCpus).build())
                        .build()
        );
        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        assertThat(containerInfo.getPassthroughAttributesMap())
                .containsEntry("titus.agent.runtimePredictionSec", "12.5")
                .containsEntry("titus.agent.runtimePredictionsAvailable", "0.1=1;0.5=5.2;0.95=12.5");
    }

    @Test
    public void jobsWithNoCommandSendFlatEntrypointStringToAgents() throws InvalidProtocolBufferException {
        DefaultV3TaskInfoFactory factory = new DefaultV3TaskInfoFactory(masterConfiguration, mock(MesosConfiguration.class));
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
        jobDescriptor = jobDescriptor.toBuilder().withContainer(jobDescriptor.getContainer().toBuilder()
                .withEntryPoint(Arrays.asList("some", "entrypoint"))
                .withCommand(null)
                .build()
        ).build();

        Protos.TaskInfo taskInfo = buildTaskInfo(factory, jobDescriptor);
        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        assertThat(containerInfo.getEntrypointStr()).isEqualTo("some entrypoint");
        assertThat(containerInfo.hasProcess()).isFalse();
    }

    @Test
    public void jobsWithCommandDoNotSendFlatStrings() throws InvalidProtocolBufferException {
        DefaultV3TaskInfoFactory factory = new DefaultV3TaskInfoFactory(masterConfiguration, mock(MesosConfiguration.class));
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
        jobDescriptor = jobDescriptor.toBuilder().withContainer(jobDescriptor.getContainer().toBuilder()
                .withEntryPoint(Arrays.asList("some", "entrypoint"))
                .withCommand(Arrays.asList("some", "command"))
                .build()
        ).build();

        Protos.TaskInfo taskInfo = buildTaskInfo(factory, jobDescriptor);
        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        assertThat(containerInfo.hasEntrypointStr()).isFalse();
        assertThat(containerInfo.getProcess().getEntrypointList()).containsExactly("some", "entrypoint");
        assertThat(containerInfo.getProcess().getCommandList()).containsExactly("some", "command");
    }

    @Test
    public void jobsAttributeDisablesShellParsingForEntryPoint() throws InvalidProtocolBufferException {
        DefaultV3TaskInfoFactory factory = new DefaultV3TaskInfoFactory(masterConfiguration, mock(MesosConfiguration.class));
        JobDescriptor<BatchJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskBatchJobDescriptor();
        Map<String, String> jobAttributesMap = new HashMap<>();
        jobAttributesMap.put(JobAttributes.JOB_PARAMETER_ATTRIBUTES_ENTRY_POINT_SKIP_SHELL_PARSING, "true");
        jobDescriptor = jobDescriptor.toBuilder().withContainer(jobDescriptor.getContainer().toBuilder()
                .withEntryPoint(Arrays.asList("some", "entrypoint"))
                .withCommand(Collections.emptyList())
                .build())
                .withAttributes(jobAttributesMap)
                .build();

        Protos.TaskInfo taskInfo = buildTaskInfo(factory, jobDescriptor);
        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        assertThat(containerInfo.hasEntrypointStr()).isFalse();
        assertThat(containerInfo.getProcess().getEntrypointList()).containsExactly("some", "entrypoint");
    }


    private Protos.TaskInfo buildTaskInfo(DefaultV3TaskInfoFactory factory, JobDescriptor<BatchJobExt> jobDescriptor) {
        return buildTaskInfo(factory, jobDescriptor, Collections.emptyMap(), Optional.empty());
    }

    private Protos.TaskInfo buildTaskInfo(DefaultV3TaskInfoFactory factory, JobDescriptor<BatchJobExt> jobDescriptor,
                                          Map<String, String> passthroughAttributes, Optional<Duration> runtimePrediction) {
        DataGenerator<Job<BatchJobExt>> jobs = JobGenerator.batchJobs(jobDescriptor);
        Job<BatchJobExt> job = jobs.getValue();
        DataGenerator<BatchJobTask> tasks = JobGenerator.batchTasks(job);
        BatchJobTask task = tasks.getValue();
        V3ConstraintEvaluatorTransformer transformer = new V3ConstraintEvaluatorTransformer(masterConfiguration,
                mock(SchedulerConfiguration.class), new TaskCache(mock(TitusRuntime.class), mock(V3JobOperations.class)),
                mock(AgentManagementService.class));

        V3QueueableTask fenzoTask = new V3QueueableTask(Tier.Flex, null, job, task, runtimePrediction,
                () -> true,
                () -> Collections.singleton(task.getId()),
                transformer, mock(SystemSoftConstraint.class),
                mock(SystemHardConstraint.class)
        );
        Protos.SlaveID agentId = Protos.SlaveID.newBuilder()
                .setValue("someAgent")
                .build();
        PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult = new PreferentialNamedConsumableResourceSet.ConsumeResult(
                0, "someAgent", "someResource", 1.0
        );
        return factory.newTaskInfo(fenzoTask, job, task, "someHost", Collections.emptyMap(), agentId,
                consumeResult, Optional.empty(), passthroughAttributes);
    }
}
