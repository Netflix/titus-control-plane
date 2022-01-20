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
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Taint;

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


    public static StdKubeApiFacade mockKubeApiFacade(V1Node... nodes) {
        StdKubeApiFacade kubeApiFacade = mock(StdKubeApiFacade.class);
        SharedIndexInformer<V1Node> nodeInformer = mock(SharedIndexInformer.class);
        Indexer<V1Node> nodeIndexer = mock(Indexer.class);
        List<V1Node> nodeIndex = new ArrayList<>(Arrays.asList(nodes));

        when(kubeApiFacade.getNodeInformer()).thenReturn(nodeInformer);
        when(nodeInformer.getIndexer()).thenReturn(nodeIndexer);
        when(nodeIndexer.list()).thenAnswer(invocation -> nodeIndex);
        when(nodeIndexer.getByKey(anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            Optional<V1Node> nodeOpt = nodeIndex.stream().filter(node -> node.getMetadata() != null &&
                    node.getMetadata().getName() != null && node.getMetadata().getName().equals(key)).findFirst();
            return nodeOpt.orElse(null);
        });
        return kubeApiFacade;
    }

    public static V1Node newNode(String id) {
        return new V1Node()
                .metadata(new V1ObjectMeta()
                        .name(id)
                        .labels(CollectionsExt.asMap(
                                NODE_LABEL_MACHINE_GROUP, "serverGroup1"
                        ))
                )
                .spec(new V1NodeSpec()
                        .taints(new ArrayList<>())
                );
    }

    public static void addNodeCondition(V1Node node, String conditionType, String conditionValue) {
        V1NodeCondition v1NodeCondition = new V1NodeCondition().type(conditionType).status(conditionValue)
                .message("Msg for " + conditionType).reason("Reason for " + conditionType);
        if (node.getStatus() != null) {
            node.getStatus().addConditionsItem(v1NodeCondition);
        } else {
            V1NodeStatus v1NodeStatus = new V1NodeStatus().addConditionsItem(v1NodeCondition);
            node.setStatus(v1NodeStatus);
        }
    }

    public static void addNodeTaint(V1Node node, String key, String value, String effect) {
        V1Taint v1Taint = new V1Taint();
        v1Taint.setKey(key);
        v1Taint.setValue(value);
        v1Taint.setEffect(effect);
        if (node.getSpec().getTaints() != null) {
            node.getSpec().getTaints().add(v1Taint);
        }
    }

}
