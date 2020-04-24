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

package com.netflix.titus.master.mesos.kubeapiserver;

/**
 * Miscellaneous Kube constants.
 */
public final class KubeConstants {
    private KubeConstants() {
    }

    /*
     * Standard node labels.
     */

    /**
     * Prefix used for legacy node attributes
     *
     * <em>Deprecated</em>: use a prefix that is a valid DNS name (rather than using Java package conventions)
     */
    @Deprecated
    public static final String NODE_ANNOTATION_PREFIX = "com.netflix.titus.agent.attribute/";

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
    public static final String TITUS_TAINT_DOMAIN = "node.titus.netflix.com/";

    /**
     * Set value to 'fenzo' to assign a machine to Fenzo scheduler. By default all machines belong to the Kube scheduler.
     */
    public static final String TAINT_SCHEDULER = TITUS_TAINT_DOMAIN + "scheduler";

    public static final String TAINT_SCHEDULER_VALUE_FENZO = "fenzo";

    public static final String TAINT_SCHEDULER_VALUE_KUBE = "kubeScheduler";

    /**
     * Machine in a farzone have the farzone taint set with its name as a value.
     */
    public static final String TAINT_FARZONE = TITUS_TAINT_DOMAIN + "farzone";

    /**
     * Machines with the taint value 'flex' belong to the flex tier. Machines with the taint 'critical' belong to
     * the critical tiers.
     */
    public static final String TAINT_TIER = TITUS_TAINT_DOMAIN + "tier";

    /**
     * Taint added to each GPU instance.
     */
    public static final String TAINT_GPU_INSTANCE = TITUS_TAINT_DOMAIN + "gpu";

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

}
