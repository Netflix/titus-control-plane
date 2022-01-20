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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LABEL_MACHINE_GROUP;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.TAINT_EFFECT_NO_EXECUTE;

public class KubernetesNodeDataResolver implements NodeDataResolver {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesNodeDataResolver.class);

    private static final long NOT_SYNCED_STALENESS_MS = 10 * 3600_000;

    private final RelocationConfiguration configuration;
    private final SharedIndexInformer<V1Node> nodeInformer;
    private final Predicate<V1Node> nodeFilter;

    private final Function<String, Matcher> relocationRequiredTaintsMatcher;
    private final Function<String, Matcher> relocationRequiredImmediatelyTaintsMatcher;
    private final Function<String, Matcher> badConditionMatcherFactory;
    private final Function<String, Matcher> badTaintMatcherFactory;

    public KubernetesNodeDataResolver(RelocationConfiguration configuration,
                                      StdKubeApiFacade kubeApiFacade,
                                      Predicate<V1Node> nodeFilter) {
        this.configuration = configuration;
        this.nodeInformer = kubeApiFacade.getNodeInformer();
        this.relocationRequiredTaintsMatcher = RegExpExt.dynamicMatcher(
                configuration::getNodeRelocationRequiredTaints,
                "nodeRelocationRequiredTaints",
                Pattern.DOTALL,
                logger);
        this.relocationRequiredImmediatelyTaintsMatcher = RegExpExt.dynamicMatcher(
                configuration::getNodeRelocationRequiredImmediatelyTaints,
                "nodeRelocationRequiredImmediatelyTaints",
                Pattern.DOTALL,
                logger);
        this.nodeFilter = nodeFilter;
        this.badConditionMatcherFactory = RegExpExt.dynamicMatcher(configuration::getBadNodeConditionPattern,
                "titus.relocation.badNodeConditionPattern", Pattern.DOTALL, logger);
        this.badTaintMatcherFactory = RegExpExt.dynamicMatcher(configuration::getBadTaintsPattern,
                "titus.relocation.badTaintsPattern", Pattern.DOTALL, logger);

    }

    @Override
    public Map<String, Node> resolve() {
        List<V1Node> k8sNodes = nodeInformer.getIndexer().list().stream().filter(nodeFilter).collect(Collectors.toList());
        Map<String, Node> result = new HashMap<>();
        k8sNodes.forEach(k8Node -> toReconcilerNode(k8Node).ifPresent(node -> result.put(node.getId(), node)));
        return result;
    }

    private Optional<Node> toReconcilerNode(V1Node k8sNode) {
        if (k8sNode.getMetadata() == null
                || k8sNode.getMetadata().getName() == null
                || k8sNode.getMetadata().getLabels() == null
                || k8sNode.getSpec() == null
                || k8sNode.getSpec().getTaints() == null) {
            return Optional.empty();
        }

        Map<String, String> k8sLabels = k8sNode.getMetadata().getLabels();
        String serverGroupId = k8sLabels.get(NODE_LABEL_MACHINE_GROUP);
        if (serverGroupId == null) {
            return Optional.empty();
        }

        boolean hasBadNodeCondition = NodePredicates.hasBadCondition(k8sNode, badConditionMatcherFactory,
                configuration.getNodeConditionTransitionTimeThresholdSeconds());

        boolean hasBadTaint = NodePredicates.hasBadTaint(k8sNode, badTaintMatcherFactory,
                configuration.getNodeTaintTransitionTimeThresholdSeconds());

        Node node = Node.newBuilder()
                .withId(k8sNode.getMetadata().getName())
                .withServerGroupId(serverGroupId)
                .withRelocationRequired(anyNoExecuteMatch(k8sNode, relocationRequiredTaintsMatcher))
                .withRelocationRequiredImmediately(anyNoExecuteMatch(k8sNode, relocationRequiredImmediatelyTaintsMatcher))
                .withBadCondition(hasBadNodeCondition || hasBadTaint)
                .build();
        return Optional.of(node);
    }

    private boolean anyNoExecuteMatch(V1Node k8sNode, Function<String, Matcher> taintsMatcher) {
        return k8sNode.getSpec().getTaints().stream().anyMatch(taint ->
                TAINT_EFFECT_NO_EXECUTE.equals(taint.getEffect()) && taintsMatcher.apply(taint.getKey()).matches()
        );
    }

    /**
     * Kubernetes informer does not provide staleness details, just information about the first sync.
     */
    @Override
    public long getStalenessMs() {
        return nodeInformer.hasSynced() ? 0 : NOT_SYNCED_STALENESS_MS;
    }
}
