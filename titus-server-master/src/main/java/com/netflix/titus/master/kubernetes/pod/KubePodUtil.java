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

package com.netflix.titus.master.kubernetes.pod;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.master.mesos.kubeapiserver.PerformanceToolUtil;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubePodUtil {
    private static final Logger logger = LoggerFactory.getLogger(KubePodUtil.class);

    private static final JsonFormat.Printer grpcJsonPrinter = JsonFormat.printer().includingDefaultValueFields();

    public static Map<String, String> createPodAnnotations(
            Job<?> job,
            Task task,
            byte[] containerInfoData,
            Map<String, String> passthroughAttributes,
            boolean includeJobDescriptor
    ) {
        String encodedContainerInfo = Base64.getEncoder().encodeToString(containerInfoData);

        Map<String, String> annotations = new HashMap<>(passthroughAttributes);
        annotations.putAll(PerformanceToolUtil.toAnnotations(job));
        annotations.put("containerInfo", encodedContainerInfo);
        Evaluators.acceptNotNull(
                job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC),
                runtimeInSec -> annotations.put(KubeConstants.JOB_RUNTIME_PREDICTION, runtimeInSec + "s")
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                count -> annotations.put(KubeConstants.OPPORTUNISTIC_CPU_COUNT, count)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION),
                id -> annotations.put(KubeConstants.OPPORTUNISTIC_ID, id)
        );
        Evaluators.acceptNotNull(
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID),
                id -> annotations.put(KubeConstants.STATIC_IP_ALLOCATION_ID, id)
        );

        annotations.putAll(createEbsPodAnnotations(job, task));

        if (includeJobDescriptor) {
            JobDescriptor grpcJobDescriptor = GrpcJobManagementModelConverters.toGrpcJobDescriptor(job.getJobDescriptor());
            try {
                String jobDescriptorJson = grpcJsonPrinter.print(grpcJobDescriptor);
                annotations.put("jobDescriptor", StringExt.gzipAndBase64Encode(jobDescriptorJson));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Unable to convert protobuf message into json: ", e);
            }
        }

        annotations.putAll(createPodAnnotationsFromJobParameters(job));

        return annotations;
    }

    public static Map<String, String> createPodAnnotationsFromJobParameters(Job<?> job) {
        Map<String, String> annotations = new HashMap<>();
        Map<String, String> containerAttributes = job.getJobDescriptor().getContainer().getAttributes();
        Evaluators.acceptNotNull(
                containerAttributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID),
                accountId -> annotations.put(KubeConstants.POD_LABEL_ACCOUNT_ID, accountId)
        );
        Evaluators.acceptNotNull(
                containerAttributes.get(JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS),
                accountId -> annotations.put(KubeConstants.POD_LABEL_SUBNETS, accountId)
        );
        return annotations;
    }

    public static Map<String, String> createEbsPodAnnotations(Job<?> job, Task task) {
        Map<String, String> annotations = new HashMap<>();

        List<EbsVolume> ebsVolumes = job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes();
        if (ebsVolumes.isEmpty()) {
            return Collections.emptyMap();
        }
        EbsVolume ebsVolume = job.getJobDescriptor().getContainer().getContainerResources().getEbsVolumes().get(0);

        String ebsVolumeId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_EBS_VOLUME_ID);
        if (ebsVolumeId == null) {
            logger.error("Expected to find assigned EBS volume ID to task {} from volumes {}", task.getId(), ebsVolumes);
            return Collections.emptyMap();
        }

        annotations.put(KubeConstants.EBS_VOLUME_ID, ebsVolumeId);
        annotations.put(KubeConstants.EBS_MOUNT_PERMISSIONS, ebsVolume.getMountPermissions().toString());
        annotations.put(KubeConstants.EBS_MOUNT_PATH, ebsVolume.getMountPath());
        annotations.put(KubeConstants.EBS_FS_TYPE, ebsVolume.getFsType());

        return annotations;
    }
}
