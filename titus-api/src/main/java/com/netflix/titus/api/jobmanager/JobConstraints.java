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

package com.netflix.titus.api.jobmanager;

import java.util.Set;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

/**
 * Constants and functions for job soft and hard constraints. Constraint names are case insensitive.
 */
public class JobConstraints {

    /**
     * Exclusive host for a task.
     */
    public static final String EXCLUSIVE_HOST = "exclusivehost";

    /**
     * Each task from a job is placed on a different agent.
     */
    public static final String UNIQUE_HOST = "uniquehost";

    /**
     * Availability zone balancing.
     */
    public static final String ZONE_BALANCE = "zonebalance";

    /**
     * Placement restricted to an agent that belongs to active ASG.
     */
    public static final String ACTIVE_HOST = "activehost";

    /**
     * Placement restricted to a specific availability zone.
     */
    public static final String AVAILABILITY_ZONE = "availabilityzone";

    /**
     * Placement restricted to an agent with the given machine id.
     */
    public static final String MACHINE_ID = "machineid";

    /**
     * Placement restricted to an agent in the given machine group.
     */
    public static final String MACHINE_GROUP = "machinegroup";

    /**
     * Placement restricted to an agent with the given machine type.
     */
    public static final String MACHINE_TYPE = "machinetype";

    /**
     * Constrain Kubernetes pod scheduling to nodes with a given Kubernetes backend version.
     */
    public static final String KUBE_BACKEND = "kubebackend";

    /**
     * Constrain Kubernetes pod scheduling to nodes with a given CPU model.
     */
    public static final String CPU_MODEL= "cpumodel";

    /**
     * Taints that are tolerated on Kubernetes Nodes.
     */
    public static final String TOLERATION = "toleration";

    public static final Set<String> CONSTRAINT_NAMES = asSet(
            UNIQUE_HOST,
            EXCLUSIVE_HOST,
            ZONE_BALANCE,
            ACTIVE_HOST,
            AVAILABILITY_ZONE,
            MACHINE_ID,
            MACHINE_GROUP,
            MACHINE_TYPE,
            KUBE_BACKEND,
            CPU_MODEL
    );
}
