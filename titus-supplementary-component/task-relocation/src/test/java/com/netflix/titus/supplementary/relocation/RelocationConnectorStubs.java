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

package com.netflix.titus.supplementary.relocation;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.testkit.model.agent.AgentComponentStub;
import com.netflix.titus.testkit.model.eviction.EvictionComponentStub;
import com.netflix.titus.testkit.model.job.JobComponentStub;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelocationConnectorStubs {

    private final TitusRuntime titusRuntime;

    private final AgentComponentStub agentComponentStub;
    private final ReadOnlyAgentOperations agentOperations;

    private final JobComponentStub jobComponentStub;
    private final ReadOnlyJobOperations jobOperations;

    private final EvictionComponentStub evictionComponentStub;

    public RelocationConnectorStubs() {
        this(TitusRuntimes.test());
    }

    public RelocationConnectorStubs(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.agentComponentStub = AgentComponentStub.newAgentComponent();
        this.agentOperations = agentComponentStub.getAgentManagementService();

        this.jobComponentStub = new JobComponentStub(titusRuntime);
        this.jobOperations = jobComponentStub.getJobOperations();

        this.evictionComponentStub = new EvictionComponentStub(jobComponentStub, titusRuntime);
    }

    public Module getModule() {
        return new AbstractModule() {
            @Override
            protected void configure() {
                bind(TitusRuntime.class).toInstance(titusRuntime);

                bind(ReadOnlyAgentOperations.class).toInstance(agentOperations);

                bind(ReadOnlyJobOperations.class).toInstance(jobOperations);

                bind(ReadOnlyEvictionOperations.class).toInstance(evictionComponentStub.getEvictionOperations());
                bind(EvictionServiceClient.class).toInstance(evictionComponentStub.getEvictionServiceClient());

                // We care only about data staleness here
                AgentDataReplicator agentDataReplicator = mock(AgentDataReplicator.class);
                when(agentDataReplicator.getStalenessMs()).thenReturn(0L);
                bind(AgentDataReplicator.class).toInstance(agentDataReplicator);

                JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);
                when(jobDataReplicator.getStalenessMs()).thenReturn(0L);
                bind(JobDataReplicator.class).toInstance(jobDataReplicator);

                EvictionDataReplicator evictionDataReplicator = mock(EvictionDataReplicator.class);
                when(evictionDataReplicator.getStalenessMs()).thenReturn(0L);
                bind(EvictionDataReplicator.class).toInstance(evictionDataReplicator);
            }
        };
    }

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }

    public ReadOnlyJobOperations getJobOperations() {
        return jobComponentStub.getJobOperations();
    }

    public ReadOnlyEvictionOperations getEvictionOperations() {
        return evictionComponentStub.getEvictionOperations();
    }

    public EvictionServiceClient getEvictionServiceClient() {
        return evictionComponentStub.getEvictionServiceClient();
    }

    public ReadOnlyAgentOperations getAgentOperations() {
        return agentComponentStub.getAgentManagementService();
    }

    public RelocationConnectorStubs addInstanceGroup(AgentInstanceGroup instanceGroup) {
        agentComponentStub.addInstanceGroup(instanceGroup);
        return this;
    }

    public RelocationConnectorStubs addJob(Job<?> job) {
        jobComponentStub.createJobAndTasks(job);
        return this;
    }

    public void addJobAttribute(String jobId, String attributeName, Object attributeValue) {
        jobComponentStub.addJobAttribute(jobId, attributeName, "" + attributeValue);
    }

    public RelocationConnectorStubs place(String instanceGroupId, Task... tasks) {
        List<AgentInstance> agents = agentOperations.getAgentInstances(instanceGroupId);

        int counter = 0;
        for (Task task : tasks) {
            AgentInstance agent = agents.get(counter++ % agents.size());
            jobComponentStub.place(task.getId(), agent);
        }
        return this;
    }

    public RelocationConnectorStubs placeOnAgent(String agentId, Task... tasks) {
        AgentInstance agent = agentOperations.getAgentInstance(agentId);
        for (Task task : tasks) {
            jobComponentStub.place(task.getId(), agent);
        }
        return this;
    }

    public RelocationConnectorStubs setQuota(String jobId, int quota) {
        Preconditions.checkArgument(jobComponentStub.getJobOperations().getJob(jobId).isPresent());

        evictionComponentStub.setJobQuota(jobId, quota);
        return this;
    }
}
