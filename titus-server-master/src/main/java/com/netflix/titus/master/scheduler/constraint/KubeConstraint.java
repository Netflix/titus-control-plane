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

package com.netflix.titus.master.scheduler.constraint;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.client.KubeApiFacade;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1Taint;

/**
 * A system constraint that prevents launching a task on Kubernetes nodes based on conditions and taints.
 */
@Singleton
public class KubeConstraint implements SystemConstraint {

    public static final String NAME = "KubeConstraint";
    public static final String INSTANCE_NOT_FOUND_REASON = "Instance for node not found in agent management";
    public static final String NODE_NOT_FOUND_REASON = "Node not found in shared informer";
    public static final String NODE_NOT_READY_REASON = "Node ready condition is not true";
    public static final String TAINT_NOT_TOLERATED_IN_CONFIGURATION_REASON = "Node has a taint that is not configured to be tolerated";
    public static final String READY = "Ready";

    private static final Result VALID = new Result(true, null);

    private enum Failure {
        INSTANCE_NOT_FOUND(INSTANCE_NOT_FOUND_REASON),
        NODE_NOT_FOUND(NODE_NOT_FOUND_REASON),
        NODE_NOT_READY(NODE_NOT_READY_REASON),
        TAINT_NOT_TOLERATED_IN_CONFIGURATION(TAINT_NOT_TOLERATED_IN_CONFIGURATION_REASON);

        private final Result result;

        Failure(String reason) {
            this.result = new Result(false, reason);
        }

        public Result toResult() {
            return result;
        }
    }

    private static final Set<String> FAILURE_REASONS = Stream.of(KubeConstraint.Failure.values())
            .map(f -> f.toResult().getFailureReason())
            .collect(Collectors.toSet());

    public static boolean isKubeConstraintReason(String reason) {
        return reason != null && FAILURE_REASONS.contains(reason);
    }

    private final SchedulerConfiguration schedulerConfiguration;
    private final MesosConfiguration mesosConfiguration;
    private final AgentManagementService agentManagementService;
    private final KubeApiFacade kubeApiFacade;


    @Inject
    public KubeConstraint(SchedulerConfiguration schedulerConfiguration,
                          MesosConfiguration mesosConfiguration,
                          AgentManagementService agentManagementService,
                          KubeApiFacade kubeApiFacade) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.mesosConfiguration = mesosConfiguration;
        this.agentManagementService = agentManagementService;
        this.kubeApiFacade = kubeApiFacade;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void prepare() {
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (!mesosConfiguration.isKubeApiServerIntegrationEnabled()) {
            return VALID;
        }

        Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService, schedulerConfiguration.getInstanceAttributeName(), targetVM);
        if (!instanceOpt.isPresent()) {
            return Failure.INSTANCE_NOT_FOUND.toResult();
        }

        String instanceId = instanceOpt.get().getId();
        V1Node node = kubeApiFacade.getNodeInformer().getIndexer().getByKey(instanceId);
        if (node == null) {
            return Failure.NODE_NOT_FOUND.toResult();
        }

        V1NodeCondition readyCondition = null;
        if (node.getStatus() != null) {
            List<V1NodeCondition> conditions = node.getStatus().getConditions();
            if (conditions != null && !conditions.isEmpty()) {
                for (V1NodeCondition condition : node.getStatus().getConditions()) {
                    if (condition.getType().equalsIgnoreCase(READY)) {
                        readyCondition = condition;
                        break;
                    }
                }
            }
        }

        if (readyCondition == null || !Boolean.parseBoolean(readyCondition.getStatus())) {
            return Failure.NODE_NOT_READY.toResult();
        }

        Set<String> toleratedTaints = mesosConfiguration.getFenzoTaintTolerations();
        if (toleratedTaints == null || toleratedTaints.isEmpty()) {
            return VALID;
        }

        if (node.getSpec() != null) {
            List<V1Taint> taints = node.getSpec().getTaints();
            if (taints != null && !taints.isEmpty()) {
                for (V1Taint taint : taints) {
                    String taintKey = taint.getKey();
                    if (!toleratedTaints.contains(taintKey)) {
                        return Failure.TAINT_NOT_TOLERATED_IN_CONFIGURATION.toResult();
                    }
                }
            }
        }

        return VALID;
    }
}
