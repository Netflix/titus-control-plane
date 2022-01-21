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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOConnector;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeConditionBuilder;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.api.model.TaintBuilder;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Indexer;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofBatchSize;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LABEL_MACHINE_GROUP;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDataFactory {

    public static final String ACTIVE_INSTANCE_GROUP_ID = "active1";
    public static final String REMOVABLE_INSTANCE_GROUP_ID = "removable1";

    public static DisruptionBudget newSelfManagedDisruptionBudget(long relocationTimeMs) {
        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                        .withRelocationTimeMs(relocationTimeMs)
                        .build()
                )
                .build();
    }

    public static DisruptionBudget newRelocationLimitDisruptionBudget(int limit) {
        return DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(RelocationLimitDisruptionBudgetPolicy.newBuilder()
                        .withLimit(limit)
                        .build()
                )
                .build();
    }

    public static Job<BatchJobExt> newBatchJob(String jobId, int size, DisruptionBudget disruptionBudget) {
        return JobGenerator.batchJobs(oneTaskBatchJobDescriptor().toBuilder()
                .withDisruptionBudget(disruptionBudget)
                .build()
                .but(ofBatchSize(size))
        ).getValue().but(JobFunctions.withJobId(jobId));
    }

    public static RelocationConnectorStubs activeRemovableSetup() {
        return activeRemovableSetup(TitusRuntimes.test());
    }

    public static RelocationConnectorStubs activeRemovableSetup(TitusRuntime titusRuntime) {
        return new RelocationConnectorStubs(titusRuntime)
                .addActiveInstanceGroup(ACTIVE_INSTANCE_GROUP_ID, 10)
                .addRemovableInstanceGroup(REMOVABLE_INSTANCE_GROUP_ID, 10);
    }

    public static Fabric8IOConnector mockFabric8IOConnector(Node... nodes) {
        Fabric8IOConnector fabric8IOConnector = mock(Fabric8IOConnector.class);
        SharedIndexInformer<Node> nodeInformer = mock(SharedIndexInformer.class);
        Indexer<Node> nodeIndexer = mock(Indexer.class);
        List<Node> nodeIndex = new ArrayList<>(Arrays.asList(nodes));

        when(fabric8IOConnector.getNodeInformer()).thenReturn(nodeInformer);
        when(nodeInformer.getIndexer()).thenReturn(nodeIndexer);
        when(nodeIndexer.list()).thenAnswer(invocation -> nodeIndex);
        when(nodeIndexer.getByKey(anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            Optional<Node> nodeOpt = nodeIndex.stream().filter(node -> node.getMetadata() != null &&
                    node.getMetadata().getName() != null && node.getMetadata().getName().equals(key)).findFirst();
            return nodeOpt.orElse(null);
        });
        return fabric8IOConnector;
    }

    public static Node newNode(String id) {
        return new NodeBuilder()
                .withNewMetadata()
                .withName(id)
                .withLabels(CollectionsExt.asMap(NODE_LABEL_MACHINE_GROUP, "serverGroup1"))
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    public static void addNodeCondition(Node node, String conditionType, String conditionValue) {
        List<NodeCondition> conditions = Evaluators.getOrDefault(node.getStatus().getConditions(), Collections.emptyList());
        NodeCondition nodeCondition = new NodeConditionBuilder()
                .withType(conditionType)
                .withStatus(conditionValue)
                .withMessage("Msg for " + conditionType)
                .withReason("Reason for " + conditionType)
                .build();
        conditions.add(nodeCondition);
        node.getStatus().setConditions(conditions);
    }

    public static void addNodeTaint(Node node, String key, String value, String effect) {
        List<Taint> taints = Evaluators.getOrDefault(node.getSpec().getTaints(), Collections.emptyList());
        Taint taint = new TaintBuilder().withKey(key).withValue(value).withEffect(effect).build();
        taints.add(taint);
        node.getSpec().setTaints(taints);
    }
}
