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

package com.netflix.titus.master;

/**
 * Set of metric related constants that establish consistent naming convention.
 */
public class MetricConstants {

    public static final String METRIC_ROOT = "titusMaster.";

    public static final String METRIC_SUPERVISOR = METRIC_ROOT + "supervisor.";

    public static final String METRIC_CAPACITY_MANAGEMENT = METRIC_ROOT + "capacity.";

    public static final String METRIC_SCHEDULING_EVICTION = METRIC_ROOT + "eviction.";

    public static final String METRIC_LEADER = METRIC_ROOT + "leader.";

    public static final String METRIC_KUBERNETES = METRIC_ROOT + "kubernetes.";

    public static final String METRIC_KUBERNETES_CONTROLLER = METRIC_KUBERNETES + "controller.";

    public static final String METRIC_LOADBALANCER = METRIC_ROOT + "loadBalancer.";

    public static final String METRIC_JOB_MANAGER = METRIC_ROOT + "jobManager.";
}
