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

package com.netflix.titus.master.agent.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.MultiDimensionalGauge;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.master.MetricConstants;

import static java.util.Arrays.asList;

/**
 * Metrics collector for Fenzo/Kube Scheduler ASG assignments.
 */
class InstanceGroupSchedulerAssignmentMonitor {

    private static final String ROOT = MetricConstants.METRIC_AGENT + "instanceGroupSchedulerAssignment.";

    private static final ScheduleDescriptor SCHEDULER_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(InstanceGroupSchedulerAssignmentMonitor.class.getSimpleName())
            .withDescription("Metrics for instance group partitioning (Fenzo/Kube scheduler)")
            .withInitialDelay(Duration.ofSeconds(5))
            .withInterval(Duration.ofSeconds(5))
            .withTimeout(Duration.ofMinutes(5))
            .build();

    private static final String TAG_OWNER = "owner";
    private static final String TAG_TIER = "tier";
    private static final String TAG_INSTANCE_GROUP_ID = "instanceGroup";
    private static final String TAG_INSTANCE_TYPE = "instanceType";

    private final AgentManagementService agentManagementService;

    private final ScheduleReference schedulerRef;

    private final MultiDimensionalGauge instanceGroupGauge;

    InstanceGroupSchedulerAssignmentMonitor(AgentManagementService agentManagementService,
                                            TitusRuntime titusRuntime) {
        this.agentManagementService = agentManagementService;
        this.schedulerRef = titusRuntime.getLocalScheduler().schedule(
                SCHEDULER_DESCRIPTOR,
                context -> refresh(),
                false
        );

        Registry registry = titusRuntime.getRegistry();
        this.instanceGroupGauge = SpectatorExt.multiDimensionalGauge(
                registry.createId(ROOT + "instanceGroups"),
                asList(TAG_OWNER, TAG_TIER, TAG_INSTANCE_GROUP_ID, TAG_INSTANCE_TYPE),
                registry
        );
    }

    void shutdown() {
        schedulerRef.cancel();
        instanceGroupGauge.remove();
    }

    private void refresh() {
        MultiDimensionalGauge.Setter setter = instanceGroupGauge.beginUpdate();
        List<AgentInstanceGroup> instanceGroups = agentManagementService.getInstanceGroups();
        instanceGroups.forEach(instanceGroup -> {
            List<String> tagValueList = new ArrayList<>();

            tagValueList.add(TAG_OWNER);
            if (agentManagementService.isOwnedByFenzo(instanceGroup)) {
                tagValueList.add("fenzo");
            } else {
                tagValueList.add("kubeScheduler");
            }

            tagValueList.add(TAG_TIER);
            tagValueList.add(instanceGroup.getTier().name());

            tagValueList.add(TAG_INSTANCE_GROUP_ID);
            tagValueList.add(instanceGroup.getId());

            tagValueList.add(TAG_INSTANCE_TYPE);
            tagValueList.add(instanceGroup.getInstanceType());

            setter.set(tagValueList, 1);
        });

        setter.commit();
    }
}
