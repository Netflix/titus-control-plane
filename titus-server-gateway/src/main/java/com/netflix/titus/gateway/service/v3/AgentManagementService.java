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

package com.netflix.titus.gateway.service.v3;

import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.AutoScalingRuleUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupAttributesUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLaunchEnabledUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.InstanceGroupTerminateEnabledUpdate;
import com.netflix.titus.grpc.protogen.InstanceOverrideStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import rx.Completable;
import rx.Observable;

public interface AgentManagementService {

    Observable<AgentInstanceGroups> getInstanceGroups();

    Observable<AgentInstanceGroup> getInstanceGroup(String id);

    Observable<AgentInstance> getAgentInstance(String id);

    Observable<AgentInstances> findAgentInstances(AgentQuery query);

    Completable updateInstanceGroupTier(TierUpdate tierUpdate);

    Completable updateAutoScalingRule(AutoScalingRuleUpdate autoScalingRuleUpdate);

    Completable updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate);

    Completable updateInstanceGroupAttributes(InstanceGroupAttributesUpdate attributesUpdate);

    Completable updateInstanceOverride(InstanceOverrideStateUpdate overrideStateUpdate);

    Observable<AgentChangeEvent> observeAgents();
}
