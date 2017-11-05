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

package io.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.service.AgentManagementFunctions;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.common.util.DateTimeExt;
import org.apache.mesos.Protos;
import rx.functions.Func0;

@Singleton
public class VMOperationsImpl implements VMOperations {

    private Func0<List<JobsOnVMStatus>> jobsGetterFunc = null;
    private final ConcurrentMap<String, List<VirtualMachineCurrentState>> vmStatesMap;
    private final AgentManagementService agentManagementService;

    @Inject
    public VMOperationsImpl(AgentManagementService agentManagementService) {
        this.agentManagementService = agentManagementService;
        vmStatesMap = new ConcurrentHashMap<>();
    }

    @Override
    public void setJobsOnVMsGetter(Func0<List<JobsOnVMStatus>> func0) {
        jobsGetterFunc = func0;
    }

    @Override
    public Map<String, List<JobsOnVMStatus>> getJobsOnVMs(boolean idleOnly, boolean excludeIdle) {
        Map<String, List<JobsOnVMStatus>> result = new HashMap<>();
        if (jobsGetterFunc != null) {
            final List<JobsOnVMStatus> statusList = jobsGetterFunc.call();
            if (statusList != null && !statusList.isEmpty()) {
                for (JobsOnVMStatus status : statusList) {
                    List<JobsOnVMStatus> jobsOnVMStatuses = result.computeIfAbsent(status.getAttributeValue(), k -> new ArrayList<>());
                    boolean isIdle = status.getJobs().isEmpty();
                    if ((!idleOnly || isIdle) && (!excludeIdle || !isIdle)) {
                        jobsOnVMStatuses.add(status);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void setAgentInfos(List<VirtualMachineCurrentState> vmStates) {
        vmStatesMap.put("0", vmStates);
    }

    @Override
    public List<AgentInfo> getAgentInfos() {
        List<VirtualMachineCurrentState> vmStates = vmStatesMap.get("0");
        List<AgentInfo> agentInfos = new ArrayList<>();
        if (vmStates != null && !vmStates.isEmpty()) {
            for (VirtualMachineCurrentState s : vmStates) {
                List<VirtualMachineLease.Range> ranges = s.getCurrAvailableResources().portRanges();
                int ports = 0;
                if (ranges != null && !ranges.isEmpty()) {
                    for (VirtualMachineLease.Range r : ranges) {
                        ports += r.getEnd() - r.getBeg();
                    }
                }
                Map<String, Protos.Attribute> attributeMap = s.getCurrAvailableResources().getAttributeMap();
                Map<String, String> attributes = new HashMap<>();
                if (attributeMap != null && !attributeMap.isEmpty()) {
                    for (Map.Entry<String, Protos.Attribute> entry : attributeMap.entrySet()) {
                        attributes.put(entry.getKey(), entry.getValue().getText().getValue());
                    }
                }

                List<String> offerIds = s.getAllCurrentOffers().stream()
                        .map(offer -> offer.getId().getValue()).collect(Collectors.toList());
                agentInfos.add(new AgentInfo(
                        s.getHostname(), s.getCurrAvailableResources().cpuCores(),
                        s.getCurrAvailableResources().memoryMB(), s.getCurrAvailableResources().diskMB(),
                        ports, s.getCurrAvailableResources().getScalarValues(), attributes, s.getResourceSets().keySet(),
                        getTimeString(s.getDisabledUntil()), offerIds
                ));
            }
        }
        return agentInfos;
    }

    @Override
    public List<AgentInfo> getDetachedAgentInfos() {
        Set<String> attachedAgents = new HashSet<>();
        List<AgentInfo> agentInfos = getAgentInfos();
        agentInfos.forEach(a -> attachedAgents.add(a.getName()));

        List<VMOperations.AgentInfo> detachedAgents = new ArrayList<>();
        List<AgentInstance> allInstances = AgentManagementFunctions.getAllInstances(agentManagementService);
        for (AgentInstance agent : allInstances) {
            if (!attachedAgents.contains(agent.getIpAddress())) {
                detachedAgents.add(new VMOperations.AgentInfo(
                        agent.getIpAddress(),
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyMap(),
                        agent.getAttributes(),
                        Collections.emptySet(),
                        null,
                        Collections.emptyList()
                ));
            }
        }
        return detachedAgents;
    }

    private String getTimeString(long disabledUntil) {
        if (System.currentTimeMillis() > disabledUntil) {
            return null;
        }
        return DateTimeExt.toUtcDateTimeString(disabledUntil);
    }
}
