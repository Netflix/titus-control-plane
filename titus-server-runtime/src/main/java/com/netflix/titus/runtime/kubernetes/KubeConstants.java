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

package com.netflix.titus.runtime.kubernetes;

/**
 * Miscellaneous Kube constants.
 */
public final class KubeConstants {

    private KubeConstants() {
    }

    /**
     * Taint effect constants.
     */

    public static String TAINT_EFFECT_NO_EXECUTE = "NoExecute";

    /*
     * Standard node labels.
     */

    public static final String NODE_LABEL_REGION = "failure-domain.beta.kubernetes.io/region";

    public static final String NODE_LABEL_ZONE = "failure-domain.beta.kubernetes.io/zone";

    public static final String NODE_LABEL_INSTANCE_TYPE = "beta.kubernetes.io/instance-type";

    /**
     * Common prefix for Titus node annotations/labels and taints.
     */
    public static final String NETFLIX_DOMAIN = "netflix.com/";

    public static final String TITUS_DOMAIN = "titus.netflix.com/";

    public static final String TITUS_NODE_DOMAIN = "node.titus.netflix.com/";

    private static final String TITUS_POD_DOMAIN = "pod.titus.netflix.com/";

    public static final String TITUS_SCALER_DOMAIN = "scaler.titus.netflix.com/";

    /**
     * Common prefix for Titus V3 job API specific annotations/labels.
     */
    public static final String TITUS_V3_JOB_DOMAIN = "v3.job.titus.netflix.com/";

    /*
     * Common Titus labels and annotations.
     */

    public static final String LABEL_CAPACITY_GROUP = TITUS_DOMAIN + "capacity-group";

    /*
     * Titus pod labels and annotations.
     */

    public static final String POD_LABEL_BYTE_UNITS = TITUS_POD_DOMAIN + "byteUnits";

    public static final String POD_LABEL_ACCOUNT_ID = TITUS_POD_DOMAIN + "accountId";

    public static final String POD_LABEL_SUBNETS = TITUS_POD_DOMAIN + "subnets";

    public static final String POD_LABEL_RELOCATION_BINPACK = TITUS_POD_DOMAIN + "relocation-binpack";

    public static final String POD_LABEL_JOB_ID = TITUS_V3_JOB_DOMAIN + "job-id";

    public static final String POD_LABEL_TASK_ID = TITUS_V3_JOB_DOMAIN + "task-id";

    /*
     * Titus node labels.
     */

    public static final String NODE_LABEL_MACHINE_ID = TITUS_NODE_DOMAIN + "id";

    public static final String NODE_LABEL_MACHINE_GROUP = TITUS_NODE_DOMAIN + "asg";

    public static final String NODE_LABEL_ACCOUNT_ID = TITUS_NODE_DOMAIN + "accountId";

    public static final String NODE_LABEL_KUBE_BACKEND = TITUS_NODE_DOMAIN + "backend";

    public static final String NODE_LABEL_CPU_MODEL = TITUS_NODE_DOMAIN + "cpu-model-name";

    public static final String NODE_LABEL_RESOURCE_POOL = TITUS_SCALER_DOMAIN + "resource-pool";

    /*
     * Titus taints.
     */

    public static final String TAINT_NODE_UNINITIALIZED = TITUS_NODE_DOMAIN + "uninitialized";

    /**
     * Machines with this taint and value 'titus' can be used for pod placement.
     */
    public static final String TAINT_VIRTUAL_KUBLET = "virtual-kubelet.io/provider";

    /**
     * Set value to 'fenzo' to assign a machine to Fenzo scheduler. By default all machines belong to the Kube scheduler.
     */
    public static final String TAINT_SCHEDULER = TITUS_NODE_DOMAIN + "scheduler";

    public static final String TAINT_SCHEDULER_VALUE_FENZO = "fenzo";

    public static final String TAINT_SCHEDULER_VALUE_KUBE = "kubeScheduler";

    /**
     * Machine in a farzone have the farzone taint set with its name as a value.
     */
    public static final String TAINT_FARZONE = TITUS_NODE_DOMAIN + "farzone";

    /**
     * Machines with the taint value 'flex' belong to the flex tier. Machines with the taint 'critical' belong to
     * the critical tiers.
     */
    public static final String TAINT_TIER = TITUS_NODE_DOMAIN + "tier";

    /**
     * Taint added to each GPU instance.
     */
    public static final String TAINT_GPU_INSTANCE = TITUS_NODE_DOMAIN + "gpu";

    /**
     * Taint added to nodes with experimental backends or backends which should not be a default scheduling targets,
     * unless explicitly requested.
     */
    public static final String TAINT_KUBE_BACKEND = TITUS_NODE_DOMAIN + "backend";

    /**
     * Nodes with this taint are in the process of being decommissioned. Effects have different meaning:
     * - <code>PreferNoSchedule</code>: nodes being drained, and capacity is preserved if needed
     * - <code>NoSchedule</code>: used to avoid placing tasks with the {@link com.netflix.titus.api.jobmanager.JobConstraints#ACTIVE_HOST} onto nodes being decommissioned
     * - <code>NoExecute</code>: nodes being actively evacuated by task relocation
     */
    public static final String TAINT_NODE_DECOMMISSIONING = TITUS_NODE_DOMAIN + "decommissioning";

    /*
     * Opportunistic scheduling annotations
     */

    /**
     * Predicted runtime for a Job in a format compatible with Go's <tt>time.Duration</tt>, e.g. <tt>10s</tt>.
     * <p>
     * See <a href="https://godoc.org/time#ParseDuration"><tt>time.ParseDuration</tt></a> for more details on the format.
     */
    public static final String JOB_RUNTIME_PREDICTION = "predictions.scheduler.titus.netflix.com/runtime";
    /**
     * Opportunistic CPUs allocated to Pods (uint) during scheduling
     */
    public static final String OPPORTUNISTIC_CPU_COUNT = "opportunistic.scheduler.titus.netflix.com/cpu";
    /**
     * Opportunistic resource CRD used when allocating opportunistic resources to a Pod during scheduling
     */
    public static final String OPPORTUNISTIC_ID = "opportunistic.scheduler.titus.netflix.com/id";

    /*
     * Pod environment variable name constants.
     */

    public static final String POD_ENV_TITUS_JOB_ID = "TITUS_JOB_ID";
    public static final String POD_ENV_TITUS_TASK_ID = "TITUS_TASK_ID";
    public static final String POD_ENV_NETFLIX_EXECUTOR = "NETFLIX_EXECUTOR";
    public static final String POD_ENV_NETFLIX_INSTANCE_ID = "NETFLIX_INSTANCE_ID";
    public static final String POD_ENV_TITUS_TASK_INSTANCE_ID = "TITUS_TASK_INSTANCE_ID";
    public static final String POD_ENV_TITUS_TASK_ORIGINAL_ID = "TITUS_TASK_ORIGINAL_ID";
    public static final String POD_ENV_TITUS_TASK_INDEX = "TITUS_TASK_INDEX";

    /*
     * Label selector operators. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
     */
    public static final String SELECTOR_OPERATOR_IN = "In";
    public static final String SELECTOR_OPERATOR_NOT_IN = "NotIn";
    public static final String SELECTOR_OPERATOR_EXISTS = "Exists";
    public static final String SELECTOR_OPERATOR_DOES_NOT_EXIST = "DoesNotExist";
    public static final String SELECTOR_OPERATOR_GT = "Gt";
    public static final String SELECTOR_OPERATOR_LT = "Lt";

    /*
     * API Constants
     */
    public static final String DEFAULT_NAMESPACE = "default";
    public static final String NOT_FOUND = "Not Found";
    public static final String READY = "Ready";
    public static final String PENDING = "Pending";
    public static final String RUNNING = "Running";
    public static final String SUCCEEDED = "Succeeded";
    public static final String FAILED = "Failed";
    public static final String BACKGROUND = "Background";

    /**
     * Reconciler Event Constants
     */
    public static final String NODE_LOST = "NodeLost";

    /**
     * Volume annotations.
     */
    public static final String VOLUME_DOMAIN = "volume." + NETFLIX_DOMAIN;
    public static final String EBS_DOMAIN = "ebs." + VOLUME_DOMAIN;
    public static final String EBS_VOLUME_ID = EBS_DOMAIN + "volume-id";
    public static final String EBS_MOUNT_PERMISSIONS = EBS_DOMAIN + "mount-perm";
    public static final String EBS_MOUNT_PATH = EBS_DOMAIN + "mount-path";
    public static final String EBS_FS_TYPE = EBS_DOMAIN + "fs-type";

    /**
     * Network annotations.
     */
    public static final String NETWORK_DOMAIN = "network." + NETFLIX_DOMAIN;
    public static final String STATIC_IP_ALLOCATION_ID = NETWORK_DOMAIN + "static-ip-allocation-uuid";
    public static final String NETWORK_MODE = NETWORK_DOMAIN + "network-mode";

    /**
     * Platform Sidecar annotations, from
     * https://github.com/Netflix/titus-kube-common/blob/master/pod/annotations.go
     */
    public static final String PLATFORM_SIDECAR_SUFFIX = ".platform-sidecars.netflix.com";
    public static final String PLATFORM_SIDECAR_CHANNEL_SUFFIX = PLATFORM_SIDECAR_SUFFIX + "/channel";
    public static final String PLATFORM_SIDECAR_ARGS_SUFFIX = PLATFORM_SIDECAR_SUFFIX + "/args.";
    ;
}
