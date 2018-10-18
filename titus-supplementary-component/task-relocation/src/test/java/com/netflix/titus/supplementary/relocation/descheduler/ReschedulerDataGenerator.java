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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.testkit.model.agent.AgentComponentStub;
import com.netflix.titus.testkit.model.eviction.EvictionComponentStub;
import com.netflix.titus.testkit.model.job.JobComponentStub;

public class ReschedulerDataGenerator {

    private final AgentComponentStub agentComponentStub;
    private final ReadOnlyAgentOperations agentOperations;

    private final JobComponentStub jobComponentStub;
    private final EvictionComponentStub evictionComponentStub;

    public ReschedulerDataGenerator(TitusRuntime titusRuntime) {
        agentComponentStub = AgentComponentStub.newAgentComponent();
        agentOperations = agentComponentStub.getAgentManagementService();

        jobComponentStub = new JobComponentStub(titusRuntime);
        evictionComponentStub = new EvictionComponentStub(jobComponentStub, titusRuntime);
    }

    public ReadOnlyJobOperations getJobOperations() {
        return jobComponentStub.getJobOperations();
    }

    public ReadOnlyEvictionOperations getEvictionOperations() {
        return evictionComponentStub.getEvictionOperations();
    }

    public ReadOnlyAgentOperations getAgentOperations() {
        return agentComponentStub.getAgentManagementService();
    }

    public ReschedulerDataGenerator addInstanceGroup(AgentInstanceGroup instanceGroup) {
        agentComponentStub.addInstanceGroup(instanceGroup);
        return this;
    }

    public ReschedulerDataGenerator addJob(Job<?> job) {
        jobComponentStub.createJobAndTasks(job);
        return this;
    }

    public ReschedulerDataGenerator place(String jobId, String instanceGroupId, int... taskIndexes) {
        List<AgentInstance> agents = agentOperations.getAgentInstances(instanceGroupId);

        List<Task> tasks = jobComponentStub.getJobOperations().getTasks(jobId);
        int counter = 0;
        for (int taskIndex : taskIndexes) {
            Task task = tasks.get(taskIndex);
            AgentInstance agent = agents.get(counter++ % agents.size());
            jobComponentStub.place(task.getId(), agent);
        }
        return this;
    }

    public ReschedulerDataGenerator setQuota(String jobId, int quota) {
        Preconditions.checkArgument(jobComponentStub.getJobOperations().getJob(jobId).isPresent());

        evictionComponentStub.setQuota(jobId, quota);
        return this;
    }
}
