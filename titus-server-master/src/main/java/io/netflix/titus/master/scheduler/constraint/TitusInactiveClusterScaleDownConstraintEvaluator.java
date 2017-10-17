/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.scheduler.constraint;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.InactiveClusterScaleDownConstraintEvaluator;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.api.agent.service.AgentManagementFunctions.observeActiveInstanceGroupIds;

@Singleton
public class TitusInactiveClusterScaleDownConstraintEvaluator implements ScaleDownOrderEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(TitusInactiveClusterScaleDownConstraintEvaluator.class);

    private final InactiveClusterScaleDownConstraintEvaluator delegate;
    private final String serverGroupAttrName;
    
    private final AgentManagementService agentManagementService;
    private final TitusRuntime titusRuntime;

    private volatile Set<String> activeInstanceGroups = Collections.emptySet();

    @Inject
    public TitusInactiveClusterScaleDownConstraintEvaluator(
            MasterConfiguration config,
            AgentManagementService agentManagementService,
            TitusRuntime titusRuntime) {
        this.agentManagementService = agentManagementService;
        this.titusRuntime = titusRuntime;
        this.delegate = new InactiveClusterScaleDownConstraintEvaluator(this::isInactive);
        this.serverGroupAttrName = config.getActiveSlaveAttributeName();
    }

    @Activator
    public void enterActiveMode() {
        titusRuntime.persistentStream(observeActiveInstanceGroupIds(agentManagementService)).subscribe(this::updateActiveInstanceGroups);
    }

    @Override
    public List<Set<VirtualMachineLease>> evaluate(Collection<VirtualMachineLease> candidates) {
        List<Set<VirtualMachineLease>> result = delegate.evaluate(candidates);
        if (logger.isDebugEnabled()) {
            logger.debug("Inactive and active sets: {}", ScaleDownUtils.toCompactString(result));
        }
        return result;
    }

    private void updateActiveInstanceGroups(List<String> activeInstanceGroups) {
        logger.info("Updating TitusInactiveClusterScaleDownConstraintEvaluator active instance group list to: {}", activeInstanceGroups);
        this.activeInstanceGroups = activeInstanceGroups == null ? Collections.emptySet() : new HashSet<>(activeInstanceGroups);
    }

    private boolean isInactive(VirtualMachineLease lease) {
        if (activeInstanceGroups.isEmpty()) {
            return false;
        }
        String serverGroupName = LeaseAttributes.getOrDefault(lease, serverGroupAttrName, null);
        return serverGroupName != null && !activeInstanceGroups.contains(serverGroupName);
    }
}
