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

package com.netflix.titus.master.mesos;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitusTaskInfoCreator {
    private static final Logger logger = LoggerFactory.getLogger(TitusTaskInfoCreator.class);

    private static final String EXECUTOR_PER_TASK_LABEL = "executorpertask";
    private static final String LEGACY_EXECUTOR_NAME = "docker-executor";
    private static final String EXECUTOR_PER_TASK_EXECUTOR_NAME = "docker-per-task-executor";

    private static final String ARN_PREFIX = "arn:aws:iam::";
    private static final String ARN_SUFFIX = ":role/";
    private static final Pattern IAM_PROFILE_RE = Pattern.compile(ARN_PREFIX + "(\\d+)" + ARN_SUFFIX + "\\S+");

    private final MasterConfiguration config;
    private final JobConfiguration jobConfiguration;
    private final String iamArnPrefix;

    public TitusTaskInfoCreator(MasterConfiguration config, JobConfiguration jobConfiguration) {
        this.config = config;
        this.jobConfiguration = jobConfiguration;

        // Get the AWS account ID to use for building IAM ARNs.
        String id = System.getenv("EC2_OWNER_ID");
        if (null == id) {
            id = "default";
        }
        iamArnPrefix = ARN_PREFIX + id + ARN_SUFFIX;
    }

    public Protos.TaskInfo createTitusTaskInfo(Protos.SlaveID slaveID,
                                               List<Parameter> parameters,
                                               TitusQueuableTask<V2JobMetadata, V2WorkerMetadata> fenzoTask,
                                               List<Integer> portsAssigned,
                                               String taskInstanceId,
                                               Map<String, String> attributesMap) {
        final WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(fenzoTask.getId());
        String _taskId = fenzoTask.getId();
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
                .setValue(_taskId).build();
        Protos.CommandInfo commandInfo = Protos.CommandInfo.newBuilder().setValue(config.pathToTitusExecutor()).build();

        boolean executorPerTask = attributesMap.containsKey(EXECUTOR_PER_TASK_LABEL);
        String executorName = LEGACY_EXECUTOR_NAME;
        String executorId = LEGACY_EXECUTOR_NAME;
        if (executorPerTask) {
            executorName = EXECUTOR_PER_TASK_EXECUTOR_NAME;
            executorId = EXECUTOR_PER_TASK_EXECUTOR_NAME + "-" + _taskId;
        }

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(executorId).build())
                .setName(executorName)
                .setCommand(commandInfo)
                .build();

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder();
        taskInfoBuilder
                .setTaskId(taskId)
                .setName(taskId.getValue())
                .setExecutor(executorInfo)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getCPUs()).build()))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getMemory()).build()))
                .addResources(Protos.Resource.newBuilder()
                        .setName("disk")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getDisk()).build()));
        if (config.getUseNetworkMbpsAttribute()) {
            taskInfoBuilder
                    .addResources(Protos.Resource.newBuilder()
                            .setName("network")
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(fenzoTask.getNetworkMbps())));
        }
        // set scalars other than cpus, mem, disk
        final Map<String, Double> scalars = fenzoTask.getScalarRequests();
        if (scalars != null && !scalars.isEmpty()) {
            for (Map.Entry<String, Double> entry : scalars.entrySet()) {
                if (!Container.PRIMARY_RESOURCES.contains(entry.getKey())) { // Already set above
                    taskInfoBuilder
                            .addResources(Protos.Resource.newBuilder()
                                    .setName(entry.getKey())
                                    .setType(Protos.Value.Type.SCALAR)
                                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(entry.getValue()).build())
                            );
                }
            }
        }
        taskInfoBuilder.setSlaveId(slaveID);
        String reqPorts = "";
        if (!portsAssigned.isEmpty()) {
            final List<Integer> requestedPorts = Parameters.getPorts(parameters);
            if (requestedPorts != null && requestedPorts.size() > 0) {
                StringBuilder b = new StringBuilder();
                int p = 0;
                for (Integer port : portsAssigned) {
                    b.append(requestedPorts.get(p)).append(",");
                    p++;
                    taskInfoBuilder.addResources(
                            Protos.Resource
                                    .newBuilder()
                                    .setName("ports")
                                    .setType(Protos.Value.Type.RANGES)
                                    .setRanges(
                                            Protos.Value.Ranges
                                                    .newBuilder()
                                                    .addRange(Protos.Value.Range.newBuilder()
                                                            .setBegin(port)
                                                            .setEnd(port))));
                }
                reqPorts = b.toString().substring(0, b.length() - 1);
            }
        }
        final TitanProtos.ContainerInfo.Builder cInfoBuilder = TitanProtos.ContainerInfo.newBuilder();

        String imageDigest = Parameters.getImageDigest(parameters);
        if (imageDigest != null) {
            cInfoBuilder.setImageDigest(imageDigest);
        }
        String version = Parameters.getVersion(parameters);
        if (version != null) {
            cInfoBuilder.setVersion(version);
        }
        String entryPoint = Parameters.getEntryPoint(parameters);
        if (entryPoint != null) {
            cInfoBuilder.setEntrypointStr(entryPoint);
        }
        String appName = Parameters.getAppName(parameters);
        if (appName != null) {
            cInfoBuilder.setAppName(appName);
        }
        String jobGroupStack = Parameters.getJobGroupStack(parameters);
        if (jobGroupStack != null) {
            cInfoBuilder.setJobGroupStack(jobGroupStack);
        }
        String jobGroupDetail = Parameters.getJobGroupDetail(parameters);
        if (jobGroupDetail != null) {
            cInfoBuilder.setJobGroupDetail(jobGroupDetail);
        }
        String jobGroupSeq = Parameters.getJobGroupSeq(parameters);
        if (jobGroupSeq != null) {
            cInfoBuilder.setJobGroupSequence(jobGroupSeq);
        }
        String iamProfile = Parameters.getIamProfile(parameters);
        if (Strings.isNullOrEmpty(iamProfile)) {
            iamProfile = jobConfiguration.getDefaultIamRole();
        }
        if (IAM_PROFILE_RE.matcher(iamProfile).matches()) {
            cInfoBuilder.setIamProfile(iamProfile);
        } else {
            cInfoBuilder.setIamProfile(iamArnPrefix + iamProfile);
        }
        String imageName = Parameters.getImageName(parameters);
        List<String> securityGroups = Parameters.getSecurityGroups(parameters);
        if (securityGroups.isEmpty()) {
            securityGroups = jobConfiguration.getDefaultSecurityGroups();
        }
        EfsMount efs = Parameters.getEfs(parameters);
        List<EfsMount> efsMounts = Parameters.getEfsMounts(parameters);

        if (efsMounts != null && !efsMounts.isEmpty()) {
            for (EfsMount efsMount : efsMounts) {
                TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.RW;
                if (efsMount.getMountPerm() == EfsMount.MountPerm.RO) {
                    mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.RO;
                } else if (efsMount.getMountPerm() == EfsMount.MountPerm.WO) {
                    mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.WO;
                }

                TitanProtos.ContainerInfo.EfsConfigInfo.Builder efsCfgBldr = TitanProtos.ContainerInfo.EfsConfigInfo.newBuilder()
                        .setEfsFsId(efsMount.getEfsId())
                        .setMountPoint(efsMount.getMountPoint())
                        .setMntPerms(mountPerms);

                if (efsMount.getEfsRelativeMountPoint() != null) {
                    efsCfgBldr = efsCfgBldr.setEfsFsRelativeMntPoint(efsMount.getEfsRelativeMountPoint());
                }

                cInfoBuilder.addEfsConfigInfo(efsCfgBldr.build());
            }
        } else if (efs != null) {
            TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.RW;
            if (efs.getMountPerm() == EfsMount.MountPerm.RO) {
                mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.RO;
            } else if (efs.getMountPerm() == EfsMount.MountPerm.WO) {
                mountPerms = TitanProtos.ContainerInfo.EfsConfigInfo.MountPerms.WO;
            }
            TitanProtos.ContainerInfo.EfsConfigInfo.Builder efsCfgBldr = TitanProtos.ContainerInfo.EfsConfigInfo.newBuilder()
                    .setEfsFsId(efs.getEfsId())
                    .setMountPoint(efs.getMountPoint())
                    .setMntPerms(mountPerms);
            cInfoBuilder.addEfsConfigInfo(efsCfgBldr.build());
        }

        Map<String, String> labels = Parameters.getLabels(parameters);
        String metatronAppMetadata = labels.get("NETFLIX_APP_METADATA");
        String metatronAppSignature = labels.get("NETFLIX_APP_METADATA_SIG");
        if (metatronAppMetadata != null && metatronAppSignature != null) {
            TitanProtos.ContainerInfo.MetatronCreds.Builder metatronBldr = TitanProtos.ContainerInfo.MetatronCreds.newBuilder()
                    .setAppMetadata(metatronAppMetadata)
                    .setMetadataSig(metatronAppSignature);
            cInfoBuilder.setMetatronCreds(metatronBldr.build());
        }

        // TODO verify if this is where the info is

        cInfoBuilder
                .setImageName(imageName)
                .setContainerPorts(reqPorts);
        if (scalars != null && !scalars.isEmpty()) {
            final Double gpuD = scalars.get(Container.RESOURCE_GPU);
            if (gpuD != null) {
                cInfoBuilder.setNumGpus(gpuD.intValue());
            }
        }
        final TaskRequest.AssignedResources assignedResources = fenzoTask.getAssignedResources();
        if (!config.getDisableSecurityGroupsAssignments() && assignedResources != null) {
            String eniLabel = "" + assignedResources.getConsumedNamedResources().get(0).getIndex();
            TitanProtos.ContainerInfo.NetworkConfigInfo.Builder nwcfgBldr = TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                    .setEniLablel(eniLabel)
                    .setEniLabel(eniLabel)
                    .addAllSecurityGroups(securityGroups);
            if (config.getUseNetworkMbpsAttribute()) {
                nwcfgBldr.setBandwidthLimitMbps((int) fenzoTask.getNetworkMbps());
            }
            cInfoBuilder.setNetworkConfigInfo(nwcfgBldr.build());
        } else {
            TitanProtos.ContainerInfo.NetworkConfigInfo.Builder nwcfgBldr = TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                    .setEniLablel("0")
                    .setEniLabel("0")
                    .addAllSecurityGroups(Collections.emptyList());
            if (config.getUseNetworkMbpsAttribute()) {
                nwcfgBldr.setBandwidthLimitMbps((int) fenzoTask.getNetworkMbps());
            }
            cInfoBuilder.setNetworkConfigInfo(nwcfgBldr.build());
        }
        Map<String, String> userProvidedEnv = Collections.unmodifiableMap(Parameters.getEnv(parameters));
        // Continue to build the deprecated environmentVariable field for backwards compatibility
        Map<String, String> envVars = new HashMap<>(userProvidedEnv);
        envVars.put("TITUS_JOB_ID", jobAndWorkerId.jobId);
        envVars.put("TITUS_TASK_ID", _taskId);
        envVars.put("TITUS_TASK_INDEX", "" + jobAndWorkerId.workerIndex);
        envVars.put("TITUS_TASK_NUMBER", "" + jobAndWorkerId.workerNumber);
        envVars.put("TITAN_JOB_ID", jobAndWorkerId.jobId);
        envVars.put("TITAN_TASK_ID", _taskId);
        envVars.put("TITUS_TASK_INSTANCE_ID", taskInstanceId);
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            // filter out invalid env variables with null/empty names and null values
            if (entry.getKey() != null && !entry.getKey().isEmpty() && entry.getValue() != null) {
                cInfoBuilder.addEnvironmentVariable(
                        TitanProtos.ContainerInfo.EnvironmentVariable.newBuilder()
                                .setName(entry.getKey())
                                .setValue(entry.getValue())
                                .build()
                );
            }
        }
        cInfoBuilder.putAllUserProvidedEnv(userProvidedEnv);
        cInfoBuilder.putTitusProvidedEnv("TITUS_JOB_ID", jobAndWorkerId.jobId);
        cInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ID", _taskId);
        cInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INDEX", "" + jobAndWorkerId.workerIndex);
        cInfoBuilder.putTitusProvidedEnv("TITUS_TASK_NUMBER", "" + jobAndWorkerId.workerNumber);
        cInfoBuilder.putTitusProvidedEnv("TITAN_JOB_ID", jobAndWorkerId.jobId);
        cInfoBuilder.putTitusProvidedEnv("TITAN_TASK_ID", _taskId);
        cInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INSTANCE_ID", taskInstanceId);
        taskInfoBuilder.setData(cInfoBuilder.build().toByteString());
        return taskInfoBuilder.build();
    }

}
