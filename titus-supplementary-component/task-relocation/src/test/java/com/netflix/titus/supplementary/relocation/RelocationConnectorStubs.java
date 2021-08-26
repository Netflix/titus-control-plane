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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
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
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.testkit.model.eviction.EvictionComponentStub;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelocationConnectorStubs {

    private final TitusRuntime titusRuntime;

    private volatile int ipCounter;

    private final TestableNodeDataResolver nodeDataResolver = new TestableNodeDataResolver();

    private final JobComponentStub jobComponentStub;
    private final ReadOnlyJobOperations jobOperations;

    private final EvictionComponentStub evictionComponentStub;

    public RelocationConnectorStubs() {
        this(TitusRuntimes.test());
    }

    public RelocationConnectorStubs(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.jobComponentStub = new JobComponentStub(titusRuntime);
        this.jobOperations = jobComponentStub.getJobOperations();
        this.evictionComponentStub = new EvictionComponentStub(jobComponentStub, titusRuntime);
    }

    public ApplicationContext getApplicationContext() {
        StaticApplicationContext context = new StaticApplicationContext();

        context.getBeanFactory().registerSingleton("titusRuntime", titusRuntime);

        context.getBeanFactory().registerSingleton("nodeDataResolver", nodeDataResolver);
        context.getBeanFactory().registerSingleton("readOnlyJobOperations", jobOperations);
        context.getBeanFactory().registerSingleton("readOnlyEvictionOperations", evictionComponentStub.getEvictionOperations());
        context.getBeanFactory().registerSingleton("evictionServiceClient", evictionComponentStub.getEvictionServiceClient());
        context.getBeanFactory().registerSingleton("jobManagementClient", mock(JobManagementClient.class));

        // We care only about data staleness here
        AgentDataReplicator agentDataReplicator = mock(AgentDataReplicator.class);
        when(agentDataReplicator.getStalenessMs()).thenReturn(0L);
        context.getBeanFactory().registerSingleton("agentOperations", agentDataReplicator);

        JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);
        when(jobDataReplicator.getStalenessMs()).thenReturn(0L);
        context.getBeanFactory().registerSingleton("jobDataReplicator", jobDataReplicator);

        EvictionDataReplicator evictionDataReplicator = mock(EvictionDataReplicator.class);
        when(evictionDataReplicator.getStalenessMs()).thenReturn(0L);
        context.getBeanFactory().registerSingleton("evictionDataReplicator", evictionDataReplicator);

        context.refresh();

        return context;
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

    public NodeDataResolver getNodeDataResolver() {
        return nodeDataResolver;
    }

    public RelocationConnectorStubs addActiveInstanceGroup(String serverGroupId, int size) {
        for (int i = 0; i < size; i++) {
            nodeDataResolver.addNode(Node.newBuilder()
                    .withId(serverGroupId + "#" + i)
                    .withIpAddress(nextIpAddress())
                    .withServerGroupId(serverGroupId)
                    .build()
            );
        }
        return this;
    }

    public RelocationConnectorStubs addRemovableInstanceGroup(String serverGroupId, int size) {
        for (int i = 0; i < size; i++) {
            nodeDataResolver.addNode(Node.newBuilder()
                    .withId(serverGroupId + "#" + i)
                    .withIpAddress(nextIpAddress())
                    .withServerGroupId(serverGroupId)
                    .withRelocationRequired(true)
                    .build()
            );
        }
        return this;
    }

    public RelocationConnectorStubs addJob(Job<?> job) {
        jobComponentStub.createJobAndTasks(job);
        return this;
    }

    public void addJobAttribute(String jobId, String attributeName, Object attributeValue) {
        jobComponentStub.addJobAttribute(jobId, attributeName, "" + attributeValue);
    }

    public void addTaskAttribute(String taskId, String attributeName, Object attributeValue) {
        jobComponentStub.addTaskAttribute(taskId, attributeName, "" + attributeValue);
    }

    public RelocationConnectorStubs place(String instanceGroupId, Task... tasks) {
        List<Node> nodes = new ArrayList<>(nodeDataResolver.getNodes(instanceGroupId).values());
        int counter = 0;
        for (Task task : tasks) {
            Node node = nodes.get(counter++ % nodes.size());
            jobComponentStub.place(task.getId(), node.getId(), node.getIpAddress());
        }
        return this;
    }

    public RelocationConnectorStubs placeOnAgent(String agentId, Task... tasks) {
        Node agent = nodeDataResolver.getNode(agentId);
        for (Task task : tasks) {
            jobComponentStub.place(task.getId(), agent.getId(), agent.getIpAddress());
        }
        return this;
    }

    public RelocationConnectorStubs setQuota(String jobId, int quota) {
        Preconditions.checkArgument(jobComponentStub.getJobOperations().getJob(jobId).isPresent());

        evictionComponentStub.setJobQuota(jobId, quota);
        return this;
    }

    public void markNodeRelocationRequired(String nodeId) {
        Node node = Preconditions.checkNotNull(nodeDataResolver.getNode(nodeId));
        nodeDataResolver.addNode(node.toBuilder().withRelocationRequired(true).build());
    }

    private String nextIpAddress() {
        return "1.1.1." + ipCounter++;
    }
}
