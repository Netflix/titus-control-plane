/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.ext.k8s.clustermembership.connector.action.K8LeaderElectionActions;
import com.netflix.titus.ext.k8s.clustermembership.connector.action.K8RegistrationActions;
import reactor.core.publisher.Mono;

class K8ClusterMembershipStateReconciler implements Function<K8ClusterState, List<Mono<Function<K8ClusterState, K8ClusterState>>>> {

    private final K8ConnectorConfiguration configuration;

    private final Clock clock;
    private final TitusRuntime titusRuntime;
    private final K8Context context;

    K8ClusterMembershipStateReconciler(K8Context context, K8ConnectorConfiguration configuration) {
        this.context = context;
        this.configuration = configuration;
        this.titusRuntime = context.getTitusRuntime();
        this.clock = titusRuntime.getClock();
    }

    @Override
    public List<Mono<Function<K8ClusterState, K8ClusterState>>> apply(K8ClusterState k8ClusterState) {

        // Check leader election process.

        boolean inLeaderElectionProcess = context.getK8LeaderElectionExecutor().isInLeaderElectionProcess();
        if (k8ClusterState.isInLeaderElectionProcess()) {
            if (!inLeaderElectionProcess) {
                return Collections.singletonList(K8LeaderElectionActions.createJoinLeadershipGroupAction(context));
            }
        } else {
            if (inLeaderElectionProcess) {
                return Collections.singletonList(K8LeaderElectionActions.createLeaveLeadershipGroupAction(context, false));
            }
        }

        // Refresh registration data so it does not look stale to other cluster members

        if (k8ClusterState.isRegistered() && k8ClusterState.getLocalMemberRevision().getTimestamp() + configuration.getReRegistrationIntervalMs() < clock.wallTime()) {
            return Collections.singletonList(K8RegistrationActions.register(context, k8ClusterState, clusterMember ->
                    ClusterMembershipRevision.<ClusterMember>newBuilder()
                            .withCurrent(clusterMember)
                            .withTimestamp(titusRuntime.getClock().wallTime())
                            .withCode("reregistered")
                            .withMessage("Registration update")
                            .build()));
        }

        return Collections.emptyList();
    }
}
