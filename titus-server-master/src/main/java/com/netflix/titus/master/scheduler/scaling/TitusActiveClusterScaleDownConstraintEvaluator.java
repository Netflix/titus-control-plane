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

package com.netflix.titus.master.scheduler.scaling;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.SchedulerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.agent.service.AgentManagementFunctions.observeActiveInstanceGroupIds;

@Singleton
//TODO remove this class once autoscaling logic moves out of Fenzo
public class TitusActiveClusterScaleDownConstraintEvaluator implements ScaleDownOrderEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(TitusActiveClusterScaleDownConstraintEvaluator.class);
    private static final List<Set<VirtualMachineLease>> EMPTY_RESULT = Collections.singletonList(Collections.emptySet());

    private final String serverGroupAttrName;

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final TitusRuntime titusRuntime;

    private volatile Set<String> activeInstanceGroups = Collections.emptySet();

    @Inject
    public TitusActiveClusterScaleDownConstraintEvaluator(
            MasterConfiguration config,
            SchedulerConfiguration schedulerConfiguration,
            AgentManagementService agentManagementService,
            TitusRuntime titusRuntime) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.titusRuntime = titusRuntime;
        this.serverGroupAttrName = config.getActiveSlaveAttributeName();
    }

    @Activator
    public void enterActiveMode() {
        titusRuntime.persistentStream(observeActiveInstanceGroupIds(agentManagementService)).subscribe(this::updateActiveInstanceGroups);
    }

    @Override
    public List<Set<VirtualMachineLease>> evaluate(Collection<VirtualMachineLease> candidates) {
        if (!schedulerConfiguration.isFenzoDownScalingEnabled()) {
            return EMPTY_RESULT;
        }
        Set<VirtualMachineLease> activeCandidates = new HashSet<>();
        for (VirtualMachineLease candidate : candidates) {
            if (!isInactive(candidate)) {
                activeCandidates.add(candidate);
            }
        }
        List<Set<VirtualMachineLease>> result = Collections.singletonList(activeCandidates);
        if (logger.isDebugEnabled()) {
            logger.debug("Active sets: {}", ScaleDownUtils.toCompactString(result));
        }
        return result;
    }

    private void updateActiveInstanceGroups(List<String> activeInstanceGroups) {
        logger.info("Updating TitusActiveClusterScaleDownConstraintEvaluator active instance group list to: {}", activeInstanceGroups);
        this.activeInstanceGroups = activeInstanceGroups == null ? Collections.emptySet() : new HashSet<>(activeInstanceGroups);
    }

    private boolean isInactive(VirtualMachineLease lease) {
        if (activeInstanceGroups.isEmpty()) {
            return false;
        }
        String serverGroupName = SchedulerUtils.getAttributeValueOrDefault(lease, serverGroupAttrName, null);
        return serverGroupName != null && !activeInstanceGroups.contains(serverGroupName);
    }
}