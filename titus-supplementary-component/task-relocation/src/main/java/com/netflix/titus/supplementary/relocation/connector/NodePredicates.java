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

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOUtil;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.Taint;

public class NodePredicates {

    public static Predicate<Node> getKubeSchedulerNodePredicate() {
        return node -> isOwnedByScheduler("kubeScheduler", node);
    }

    @VisibleForTesting
    static boolean isOwnedByScheduler(String schedulerName, Node node) {
        if (node == null || node.getSpec() == null || node.getSpec().getTaints() == null) {
            return false;
        }

        List<Taint> taints = node.getSpec().getTaints();
        return taints.stream().anyMatch(taint ->
                KubeConstants.TAINT_SCHEDULER.equals(taint.getKey()) && schedulerName.equals(taint.getValue())
        );
    }

    @VisibleForTesting
    static boolean hasBadCondition(Node node, Function<String, Matcher> badConditionExpression,
                                   int nodeConditionTransitionTimeThresholdSeconds) {

        if (node.getStatus() != null && node.getStatus().getConditions() != null) {
            return node.getStatus().getConditions().stream()
                    .anyMatch(v1NodeCondition -> badConditionExpression.apply(v1NodeCondition.getType()).matches() &&
                            Boolean.parseBoolean(v1NodeCondition.getStatus()) &&
                            !isNodeConditionTransitionedRecently(v1NodeCondition, nodeConditionTransitionTimeThresholdSeconds));
        }
        return false;
    }

    @VisibleForTesting
    static boolean hasBadTaint(Node node, Function<String, Matcher> badTaintExpression,
                               int nodeTaintTransitionTimeThresholdSeconds) {
        if (node.getSpec() != null && node.getSpec().getTaints() != null) {
            return node.getSpec().getTaints().stream()
                    .anyMatch(v1Taint -> badTaintExpression.apply(v1Taint.getKey()).matches() &&
                            matchesTaintValueIfAvailable(v1Taint, Boolean.TRUE.toString()) &&
                            !isTransitionedRecently(v1Taint.getTimeAdded(), nodeTaintTransitionTimeThresholdSeconds));
        }
        return false;
    }

    static boolean matchesTaintValueIfAvailable(Taint taint, String value) {
        if (taint.getValue() != null) {
            return taint.getValue().equalsIgnoreCase(value);
        }
        return true;
    }


    static boolean isNodeConditionTransitionedRecently(NodeCondition nodeCondition, int thresholdSeconds) {
        OffsetDateTime threshold = OffsetDateTime.now().minusSeconds(thresholdSeconds);
        if (nodeCondition.getLastTransitionTime() != null) {
            OffsetDateTime timestamp = Fabric8IOUtil.parseTimestamp(nodeCondition.getLastTransitionTime());
            return timestamp.isAfter(threshold);
        }
        return false;
    }

    static boolean isTransitionedRecently(String nodeTransitionTime, int thresholdSeconds) {
        OffsetDateTime threshold = OffsetDateTime.now().minusSeconds(thresholdSeconds);
        if (nodeTransitionTime != null) {
            OffsetDateTime timestamp = Fabric8IOUtil.parseTimestamp(nodeTransitionTime);
            return timestamp.isAfter(threshold);
        }
        return false;
    }
}
