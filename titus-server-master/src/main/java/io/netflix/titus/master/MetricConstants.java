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

package io.netflix.titus.master;

/**
 * Set of metric related constants that establish consistent naming convention.
 */
public class MetricConstants {

    public static final String METRIC_ROOT = "titusMaster.";

    public static final String METRIC_AGENT = METRIC_ROOT + "agent.";

    public static final String METRIC_AGENT_CACHE = METRIC_ROOT + "agentCache.";

    public static final String METRIC_AGENT_MONITOR = METRIC_ROOT + "agentMonitor.";

    public static final String METRIC_CAPACITY_MANAGEMENT = METRIC_ROOT + "capacity.";

    public static final String METRIC_SCHEDULING_SERVICE = METRIC_ROOT + "scheduler.";

    public static final String METRIC_SCHEDULING_JOB = METRIC_ROOT + "job.";

    public static final String METRIC_LEADER = METRIC_ROOT + "leader.";

    public static final String METRIC_MESOS = METRIC_ROOT + "mesos.";

    public static final String METRIC_STORE = METRIC_ROOT + "store.";

    public static final String METRIC_TASK_MIGRATION = METRIC_ROOT + "taskMigration.";

    public static final String METRIC_WORKER_STATE_OBSERVER = METRIC_ROOT + "workerStateObserver.";
}
