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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

/**
 * Miscellaneous Kube constants.
 */
public class KubeConstants {

    /*
     * Standard node labels.
     */

    public static final String NODE_LABEL_REGION = "failure-domain.beta.kubernetes.io/region";

    public static final String NODE_LABEL_ZONE = "failure-domain.beta.kubernetes.io/zone";

    public static final String NODE_LABEL_INSTANCE_TYPE = "beta.kubernetes.io/instance-type";

    /*
     * Titus node labels.
     */

    public static final String NODE_LABEL_MACHINE_ID = "titus/machine-id";

    public static final String NODE_LABEL_MACHINE_GROUP = "titus/machine-group";

    /*
     * Titus pod labels.
     */

    public static final String POD_LABEL_JOB_ID = "titus/jobId";

    public static final String POD_LABEL_TASK_ID = "titus/taskId";

    /*
     * Titus taints.
     */

    /**
     * Machines with this taint and value 'titus' can be used for pod placement.
     */
    public static final String TAINT_VIRTUAL_KUBLET = "virtual-kubelet.io/provider";

    /**
     * Common prefix for Titus taints.
     */
    public static final String TITUS_TAINT_DOMAIN = "com.netflix.titus.agent.taint/";

    /**
     * Set value to 'fenzo' to assign a machine to Fenzo scheduler. By default all machines belong to the Kube scheduler.
     */
    public static final String TAINT_SCHEDULER = TITUS_TAINT_DOMAIN + "scheduler";

    /**
     * Machines with the taint value 'flex' belong to the flex tier. Machines with the taint 'critical' belong to
     * the critical tiers.
     */
    public static final String TAINT_TIER = TITUS_TAINT_DOMAIN + "tier";

    /**
     * Taint for GPU machines to prevent non-GPU pods running on them.
     */
    public static final String TAINT_GPU_INSTANCE = TITUS_TAINT_DOMAIN + "gpu";
}
