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

import java.util.List;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.testkit.model.agent.AgentGenerator;

/**
 * Test status values.
 */
public class StatusSamples {

    public static final AgentInstance SAMPLE_AGENT_1;
    public static final AgentInstance SAMPLE_AGENT_2;

    static {
        List<AgentInstance> instances = AgentGenerator.agentInstances().toList(2);
        SAMPLE_AGENT_1 = instances.get(0);
        SAMPLE_AGENT_2 = instances.get(1);
    }

    public static final String SAMPLE_AGENT_ID = SAMPLE_AGENT_1.getId();
    public static final String SAMPLE_AGENT2_ID = SAMPLE_AGENT_2.getId();

    public static Status STATUS_STARTED = withId(SAMPLE_AGENT_ID, new Status(
            "job-001", null, 0, 0, 0, Status.TYPE.INFO, "status Started", "#data#", V2JobState.Started
    ));

    public static Status STATUS_2_STARTED = withId(SAMPLE_AGENT2_ID, new Status(
            "job-002", null, 0, 0, 0, Status.TYPE.INFO, "status Started", "#data#", V2JobState.Started
    ));

    public static Status STATUS_FAILED = withId(SAMPLE_AGENT_ID, new Status(
            "job-001", null, 0, 0, 0, Status.TYPE.INFO, "status Failed", "#data#", V2JobState.Failed
    ));

    public static Status STATUS_CRASHED = withReason(JobCompletedReason.Lost, withId(SAMPLE_AGENT_ID,
            new Status(
                    "job-001", null, 0, 0, 0, Status.TYPE.INFO, "status Failed", "#data#", V2JobState.Failed
            )));

    private static Status withReason(JobCompletedReason reason, Status status) {
        status.setReason(reason);
        return status;
    }

    private static Status withId(String id, Status status) {
        status.setInstanceId(id);
        return status;
    }
}
