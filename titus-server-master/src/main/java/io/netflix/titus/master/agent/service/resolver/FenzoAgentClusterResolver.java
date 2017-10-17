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

package io.netflix.titus.master.agent.service.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.VirtualMachineCurrentState;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.agent.TitusAgent;
import io.netflix.titus.master.agent.TitusAgentServerGroup;
import io.netflix.titus.master.scheduler.SchedulingService;

/**
 * Resolver implementation that retrieves agent information from Fenzo.
 */
@Singleton
public class FenzoAgentClusterResolver implements AgentClusterResolver {

    private final SchedulingService schedulingService;

    @Inject
    public FenzoAgentClusterResolver(SchedulingService schedulingService) {
        this.schedulingService = schedulingService;
    }

    @Override
    public List<String> resolve() {
        List<VirtualMachineCurrentState> vms = schedulingService.getVmCurrentStates();
        if (CollectionsExt.isNullOrEmpty(vms)) {
            return Collections.emptyList();
        }
        List<String> hosts = new ArrayList<>(vms.size());
        vms.forEach(vm -> hosts.add(vm.getHostname()));
        return hosts;
    }

    @Override
    public List<TitusAgentServerGroup> resolveServerGroups() {
        return Collections.emptyList();
    }

    @Override
    public List<TitusAgent> resolveAll() {
        return resolve().stream().map(ip -> new TitusAgent("vm:" + ip, ip, Collections.emptyMap())).collect(Collectors.toList());
    }
}
