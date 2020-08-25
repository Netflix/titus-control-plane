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

package com.netflix.titus.supplementary.relocation.connector;

import java.util.List;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Taint;

public class NodePredicates {

    public static Predicate<AgentInstance> getFenzoNodePredicate(SharedIndexInformer<V1Node> nodeInformer) {
        return agentInstance -> isOwnedByScheduler("fenzo", nodeInformer.getIndexer().getByKey(agentInstance.getId()));
    }

    public static Predicate<V1Node> getKubeSchedulerNodePredicate() {
        return node -> isOwnedByScheduler("kubeScheduler", node);
    }

    @VisibleForTesting
    static boolean isOwnedByScheduler(String schedulerName, V1Node node) {
        if (node == null || node.getSpec() == null || node.getSpec().getTaints() == null) {
            return false;
        }

        List<V1Taint> taints = node.getSpec().getTaints();
        return taints.stream().anyMatch(taint ->
                KubeConstants.TAINT_SCHEDULER.equals(taint.getKey()) && schedulerName.equals(taint.getValue())
        );
    }
}
