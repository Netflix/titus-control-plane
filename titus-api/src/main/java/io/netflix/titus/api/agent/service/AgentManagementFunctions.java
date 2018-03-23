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

package io.netflix.titus.api.agent.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

import static java.util.Collections.singletonList;

/**
 * A collection of helper functions complementing {@link AgentManagementService} API.
 */
public final class AgentManagementFunctions {

    private AgentManagementFunctions() {
    }

    public static List<AgentInstance> getAllInstances(AgentManagementService service) {
        return getMatchingInstances(d -> true, service);
    }

    public static List<AgentInstance> getMatchingInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> predicate, AgentManagementService service) {
        return service.findAgentInstances(predicate).stream().flatMap(pair -> pair.getRight().stream()).collect(Collectors.toList());
    }

    /**
     * Emits lists of active instance groups.
     */
    public static Observable<List<AgentInstanceGroup>> observeActiveInstanceGroups(AgentManagementService agentManagementService) {
        return agentManagementService.events(false)
                .flatMap(event -> {
                    if (event instanceof AgentInstanceGroupUpdateEvent || event instanceof AgentInstanceGroupRemovedEvent) {
                        return Observable.just(findActiveInstanceGroups(agentManagementService));
                    }
                    return Observable.empty();
                })
                .compose(ObservableExt.head(() -> singletonList(findActiveInstanceGroups(agentManagementService))))
                .distinctUntilChanged()
                .map(ArrayList::new); // All clients expect list type not set.
    }

    /**
     * Emits lists of active instance group ids.
     */
    public static Observable<List<String>> observeActiveInstanceGroupIds(AgentManagementService agentManagementService) {
        return observeActiveInstanceGroups(agentManagementService)
                .map(instanceGroups -> instanceGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toList()));
    }

    private static SortedSet<AgentInstanceGroup> findActiveInstanceGroups(AgentManagementService agentManagementService) {
        SortedSet<AgentInstanceGroup> activeInstanceGroups = new TreeSet<>(Comparator.comparing(AgentInstanceGroup::getId));
        agentManagementService.getInstanceGroups().stream()
                .filter(AgentManagementFunctions::isActiveOrPhasedOut)
                .forEach(activeInstanceGroups::add);
        return activeInstanceGroups;
    }

    public static boolean isActiveOrPhasedOut(AgentInstanceGroup instanceGroup) {
        return instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active ||
                instanceGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.PhasedOut;
    }
}
