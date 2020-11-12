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

package com.netflix.titus.master.kubernetes.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LABEL_ACCOUNT_ID;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.READY;

/**
 * Responsible for deleting Kubernetes node objects when they no longer exist in agent management. This will eventually be moved
 * to a new component.
 */

@Singleton
public class NodeGcController extends BaseGcController<V1Node> {
    public static final String NODE_GC_CONTROLLER = "nodeGcController";
    public static final String NODE_GC_CONTROLLER_DESCRIPTION = "GC nodes that are not in agent management and are not reachable.";

    private static final Logger logger = LoggerFactory.getLogger(NodeGcController.class);
    private final AgentManagementService agentManagementService;
    private final KubeApiFacade kubeApiFacade;
    private final Clock clock;
    private final KubeControllerConfiguration kubeControllerConfiguration;

    @Inject
    public NodeGcController(
            TitusRuntime titusRuntime,
            LocalScheduler scheduler,
            @Named(NODE_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            @Named(NODE_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
            AgentManagementService agentManagementService,
            KubeApiFacade kubeApiFacade,
            KubeControllerConfiguration kubeControllerConfiguration) {
        super(
                NODE_GC_CONTROLLER,
                NODE_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );
        this.agentManagementService = agentManagementService;
        this.kubeApiFacade = kubeApiFacade;
        this.clock = titusRuntime.getClock();
        this.kubeControllerConfiguration = kubeControllerConfiguration;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getNodeInformer().hasSynced();
    }

    @Override
    public List<V1Node> getItemsToGc() {
        return kubeApiFacade.getNodeInformer().getIndexer().list().stream()
                .filter(this::isNodeInConfiguredAccount)
                .filter(this::isNodeEligibleForGc)
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1Node item) {
        return GcControllerUtil.deleteNode(kubeApiFacade, logger, item);
    }

    @VisibleForTesting
    boolean isNodeInConfiguredAccount(V1Node node) {
        if (!kubeControllerConfiguration.isVerifyNodeAccountIdEnabled()) {
            return true;
        }

        V1ObjectMeta metadata = node.getMetadata();
        if (metadata == null) {
            return false;
        }

        Map<String, String> annotations = metadata.getAnnotations();
        if (annotations == null || annotations.isEmpty()) {
            return false;
        }

        String accountId = annotations.getOrDefault(NODE_LABEL_ACCOUNT_ID, "");
        return accountId.equals(kubeControllerConfiguration.getAccountId());
    }

    @VisibleForTesting
    boolean isNodeEligibleForGc(V1Node node) {
        String nodeName = KubeUtil.getMetadataName(node.getMetadata());
        Optional<V1NodeCondition> readyNodeConditionOpt = KubeUtil.findNodeCondition(node, READY);
        if (StringExt.isNotEmpty(nodeName) && readyNodeConditionOpt.isPresent()) {
            V1NodeCondition readyNodeCondition = readyNodeConditionOpt.get();
            boolean isReadyConditionTimestampPastGracePeriod = hasConditionGracePeriodElapsed(readyNodeCondition,
                    kubeControllerConfiguration.getNodeGcGracePeriodMs());
            return isReadyConditionTimestampPastGracePeriod && isAgentInstanceNotAvailable(nodeName);
        }
        return false;
    }

    private boolean hasConditionGracePeriodElapsed(V1NodeCondition condition, long gracePeriodMs) {
        DateTime lastHeartbeatTime = condition.getLastHeartbeatTime();
        return lastHeartbeatTime != null && clock.isPast(lastHeartbeatTime.getMillis() + gracePeriodMs);
    }

    private boolean isAgentInstanceNotAvailable(String nodeName) {
        if (StringExt.isEmpty(nodeName)) {
            return false;
        }

        Optional<AgentInstance> agentInstanceOpt = agentManagementService.findAgentInstance(nodeName);
        if (!agentInstanceOpt.isPresent()) {
            // re-check with agent service to find an instance
            AgentInstance agentInstance = agentManagementService.getAgentInstanceAsync(nodeName)
                    .toBlocking()
                    .firstOrDefault(null);
            if (agentInstance != null) {
                return agentInstance.getLifecycleStatus().getState() == InstanceLifecycleState.Stopped;
            }
            return false;
        }
        AgentInstance agentInstance = agentInstanceOpt.get();
        return agentInstance.getLifecycleStatus().getState() == InstanceLifecycleState.Stopped;
    }
}