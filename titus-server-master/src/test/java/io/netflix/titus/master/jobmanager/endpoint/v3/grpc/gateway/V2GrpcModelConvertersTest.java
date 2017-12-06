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

package io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Constraints;
import com.netflix.titus.grpc.protogen.Container;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobGroupInfo;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.Owner;
import com.netflix.titus.grpc.protogen.RetryPolicy;
import com.netflix.titus.grpc.protogen.SecurityProfile;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.V2JobDurationType;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import io.netflix.titus.testkit.model.runtime.RuntimeModelGenerator;
import org.junit.Before;
import org.junit.Test;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcModelConverters.toGrpcJobConstraintList;
import static io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcModelConverters.toMachineDefinition;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.ALL_TASK_STATES;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.LABEL_LEGACY_NAME;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toJobQueryCriteria;
import static io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes.TASK_ATTRIBUTES_AGENT_REGION;
import static io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V2GrpcModelConvertersTest {

    private static final com.netflix.titus.grpc.protogen.ContainerResources GRPC_RESOURCES = com.netflix.titus.grpc.protogen.ContainerResources.newBuilder()
            .setCpu(2)
            .setGpu(1)
            .setMemoryMB(1024)
            .setDiskMB(8192)
            .setNetworkMbps(128)
            .setAllocateIP(true)
            .build();

    private static final SecurityProfile GRPC_SECURITY_PROFILE = SecurityProfile.newBuilder()
            .addSecurityGroups("sg-1").addSecurityGroups("sg-2")
            .build();

    private static final String REGION = "us-east-1";

    private final RuntimeModelGenerator generator = new RuntimeModelGenerator();

    private final MasterConfiguration configuration = mock(MasterConfiguration.class);

    private final LogStorageInfo<V2WorkerMetadata> logStorageInfo = mock(LogStorageInfo.class);

    @Before
    public void setUp() throws Exception {
        when(configuration.getRegion()).thenReturn(REGION);
        when(logStorageInfo.getLinks(any())).thenReturn(new LogStorageInfo.LogLinks(Optional.empty(), Optional.empty(), Optional.empty()));
    }

    @Test
    public void testToMachineDefinition() throws Exception {
        MachineDefinition result = toMachineDefinition(GRPC_RESOURCES);

        assertThat(result.getCpuCores()).isEqualTo(2);
        assertThat(result.getMemoryMB()).isEqualTo(1024);
        assertThat(result.getDiskMB()).isEqualTo(8 * 1024);
        assertThat(result.getNetworkMbps()).isEqualTo(128);
        assertThat(result.getScalars()).containsEntry("gpu", 1.0);
    }

    @Test
    public void testToGrpcJobConstraintList() {
        Constraints grpcConstraints = toGrpcJobConstraintList(
                asList(JobConstraints.ExclusiveHost, JobConstraints.ZoneBalance)
        );
        assertThat(grpcConstraints.getConstraintsMap()).containsEntry(JobConstraints.ExclusiveHost.name(), "true");
        assertThat(grpcConstraints.getConstraintsMap()).containsEntry(JobConstraints.ZoneBalance.name(), "true");
    }

    @Test
    public void testToJobConstraintList() {
        List<JobConstraints> jobConstraints = V2GrpcModelConverters.toJobConstraintList(Constraints.newBuilder().
                putConstraints(JobConstraints.ExclusiveHost.name(), "true").
                putConstraints(JobConstraints.ZoneBalance.name(), "true")
                .build()
        );
        assertThat(jobConstraints).containsExactlyInAnyOrder(JobConstraints.ExclusiveHost, JobConstraints.ZoneBalance);
    }

    @Test
    public void testToV2JobDefinitionForServiceJob() throws Exception {
        JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                .setOwner(Owner.newBuilder().setTeamEmail("netflixoss@netflix.com"))
                .setApplicationName("myApp")
                .setJobGroupInfo(JobGroupInfo.newBuilder().setStack("main").setDetail("canary").setSequence("v001"))
                .putAttributes("labelA", "valueA")
                .setContainer(Container.newBuilder()
                        .setResources(GRPC_RESOURCES)
                        .setSecurityProfile(GRPC_SECURITY_PROFILE)
                        .setImage(Image.newBuilder().setName("my/image").setTag("latest"))
                        .putAttributes("debugEnabled", "true")
                        .addCommand("echo 'Hello!'")
                        .putEnv("CREATOR", "titus")
                        .setHardConstraints(Constraints.newBuilder().putConstraints("UniqueHost", "true"))
                        .setSoftConstraints(Constraints.newBuilder().putConstraints("ZoneBalance", "true"))
                )
                .setService(ServiceJobSpec.newBuilder()
                        .setEnabled(true)
                        .setCapacity(Capacity.newBuilder().setMin(1).setDesired(5).setMax(10))
                        .setRetryPolicy(RetryPolicy.newBuilder().setDelayed(RetryPolicy.Delayed.newBuilder().setInitialDelayMs(1000).setDelayMs(2000).setRetries(3)))
                )
                .build();
        V2JobDefinition v2JobDefinition = V2GrpcModelConverters.toV2JobDefinition(jobDescriptor);

        assertThat(v2JobDefinition.getUser()).isEqualTo("netflixoss@netflix.com");
        assertThat(v2JobDefinition.getJobSla().getDurationType()).isEqualTo(V2JobDurationType.Perpetual);
        assertThat(v2JobDefinition.getJobSla().getRuntimeLimitSecs()).isEqualTo(Integer.MAX_VALUE);

        StageSchedulingInfo stage1 = v2JobDefinition.getSchedulingInfo().getStages().get(1);
        assertThat(stage1.getAllocateIP()).isTrue();
        assertThat(stage1.getHardConstraints()).contains(JobConstraints.UniqueHost);
        assertThat(stage1.getSoftConstraints()).contains(JobConstraints.ZoneBalance);
        expectParameters(v2JobDefinition.getParameters(),
                "type", "titus",
                "name", "myApp",
                "appName", "myApp",
                "imageName", "my/image",
                "version", "latest",
                "inService", "true",
                "entryPoint", "",
                "env", "{\"CREATOR\":\"titus\"}",
                "jobType", "Service",
                "jobGroupStack", "main",
                "jobGroupDetail", "canary",
                "jobGroupSeq", "v001",
                "labels", "{\"labelA\":\"valueA\"}",
                "securityGroups", "sg-1,sg-2"
        );
    }

    @Test
    public void testToGrpcJobDescriptor() throws Exception {
        V2JobMetadata v2Job = generator.newJobMetadata(Parameters.JobType.Batch, "myJob", 1.0);
        JobDescriptor v3JobDescriptor = V2GrpcModelConverters.toGrpcJobDescriptor(v2Job);
        compare(v3JobDescriptor, v2Job);
    }

    @Test
    public void testToGrpcTask() throws Exception {
        String v2JobId = generator.newJobMetadata(Parameters.JobType.Batch, "myJob").getJobId();
        V2WorkerMetadata task0 = generator.scheduleJob(v2JobId).getStageMetadata(1).getWorkerByIndex(0);

        Task v3Task = V2GrpcModelConverters.toGrpcTask(configuration, task0, logStorageInfo);
        compare(v3Task, task0);
    }

    @Test
    public void testToGrpcJob() throws Exception {
        V2JobMetadata v2Job = generator.newJobMetadata(Parameters.JobType.Batch, "myJob");
        Job v3Job = V2GrpcModelConverters.toGrpcJob(v2Job);

        assertThat(v3Job.getId()).isEqualTo(v2Job.getJobId());
        compare(v3Job.getJobDescriptor(), v2Job);
    }

    @Test
    public void testToJobQueryCriteria() throws Exception {
        JobQuery jobQuery = JobQuery.newBuilder()
                .putFilteringCriteria("appName", " myApp ")
                .putFilteringCriteria("imageName", " my/image ")
                .putFilteringCriteria("jobType", " Service ")
                .putFilteringCriteria("labels", " key1:value1.1,key1:value1.2, key2:value2")
                .putFilteringCriteria("labels.op", " or ")
                .putFilteringCriteria("taskStates", " StartInitiated , Started ")
                .build();

        JobQueryCriteria jobQueryCriteria = toJobQueryCriteria(jobQuery);

        assertThat(jobQueryCriteria.getAppName()).contains("myApp");
        assertThat(jobQueryCriteria.getImageName()).contains("my/image");
        assertThat(jobQueryCriteria.getJobType()).contains(JobSpecCase.SERVICE);

        Map<String, Set<String>> labels = jobQueryCriteria.getLabels();
        assertThat(labels).containsEntry("key1", asSet("value1.1", "value1.2"));
        assertThat(labels).containsEntry("key2", asSet("value2"));
        assertThat(jobQueryCriteria.isLabelsAndOp()).isFalse();

        assertThat(jobQueryCriteria.getTaskStates()).containsExactlyInAnyOrder(TaskState.StartInitiated, TaskState.Started);
    }

    @Test
    public void testToJobQueryCriteriaWithAnyTaskState() throws Exception {
        JobQuery jobQuery = JobQuery.newBuilder()
                .putFilteringCriteria("taskStates", " Any ")
                .build();

        JobQueryCriteria jobQueryCriteria = toJobQueryCriteria(jobQuery);
        assertThat(jobQueryCriteria.getTaskStates()).containsAll(ALL_TASK_STATES);
    }

    @Test
    public void testToGrpcJobSnapshotNotification() throws Exception {
        V2JobMetadata jobMetadata = generator.newJobMetadata(Parameters.JobType.Service, "myJob");
        JobChangeNotification notification = V2GrpcModelConverters.toGrpcJobNotification(jobMetadata);

        assertThat(notification.getJobUpdate().getJob().getId()).isEqualTo(jobMetadata.getJobId());
    }

    private void expectParameters(List<Parameter> parameters, String... expected) {
        for (int i = 0; i < expected.length; i += 2) {
            String name = expected[i];
            String value = expected[i + 1];
            Optional<Parameter> found = parameters.stream().filter(p -> p.getName().equals(name)).findFirst();
            assertThat(found).withFailMessage("missing parameter " + name).isPresent();
            assertThat(found.get().getValue()).isEqualTo(value);
        }
    }

    private void compare(JobDescriptor v3JobDescriptor, V2JobMetadata v2Job) {
        V2StageMetadata v2Stage = v2Job.getStageMetadata(1);

        assertThat(v3JobDescriptor.getApplicationName()).isEqualTo(Parameters.getAppName(v2Job.getParameters()));
        assertThat(v3JobDescriptor.getJobGroupInfo().getStack()).contains(Parameters.getJobGroupStack(v2Job.getParameters()));

        Map<String, String> v3Labels = v3JobDescriptor.getAttributesMap();
        assertThat(v3Labels).containsEntry(LABEL_LEGACY_NAME, v2Job.getName());

        assertThat(v3JobDescriptor.getContainer()).isNotNull();
        Container container = v3JobDescriptor.getContainer();
        assertThat(container.getImage().getName()).isEqualTo("testImage");
        assertThat(container.getImage().getTag()).isEqualTo("latest");
        assertThat(container.getResources().getCpu()).isEqualTo(v2Stage.getMachineDefinition().getCpuCores());
        assertThat(container.getResources().getMemoryMB()).isEqualTo((int) v2Stage.getMachineDefinition().getMemoryMB());
        assertThat(container.getResources().getDiskMB()).isEqualTo((int) v2Stage.getMachineDefinition().getDiskMB());
        assertThat(container.getResources().getNetworkMbps()).isEqualTo((int) v2Stage.getMachineDefinition().getNetworkMbps());
        Map<String, Double> scalars = v2Stage.getMachineDefinition().getScalars();
        int gpu = 0;
        if (scalars != null) {
            Double scalarGpu = scalars.get("gpu");
            if (scalarGpu != null) {
                gpu = scalarGpu.intValue();
            }
        }
        assertThat(container.getResources().getGpu()).isEqualTo(gpu);
        assertThat(container.getEntryPointList().get(0)).isEqualTo(Parameters.getEntryPoint(v2Job.getParameters()));
        assertThat(container.getEnvMap()).isEqualTo(Parameters.getEnv(v2Job.getParameters()));
        assertThat(container.getSoftConstraints().getConstraintsCount()).isEqualTo(v2Stage.getSoftConstraints().size());
        assertThat(container.getHardConstraints().getConstraintsCount()).isEqualTo(v2Stage.getHardConstraints().size());
    }

    private void compare(Task v3Task, V2WorkerMetadata v2Task) {
        String v2TaskId = WorkerNaming.getTaskId(v2Task);
        assertThat(v3Task.getId()).isEqualTo(v2TaskId);
        assertThat(v3Task.getStatus()).isEqualTo(V2GrpcModelConverters.toGrpcTaskStatus(v2Task));
        assertThat(v3Task.getStatusHistoryList()).isEqualTo(V2GrpcModelConverters.toGrpcTaskStatusHistory(v2Task));

        Map<String, String> context = v3Task.getTaskContextMap();
        assertThat(context.get(TASK_ATTRIBUTES_AGENT_REGION)).isEqualTo(configuration.getRegion());
        assertThat(context.get(TASK_ATTRIBUTES_TASK_INDEX)).isEqualTo(Integer.toString(v2Task.getWorkerIndex()));
    }
}