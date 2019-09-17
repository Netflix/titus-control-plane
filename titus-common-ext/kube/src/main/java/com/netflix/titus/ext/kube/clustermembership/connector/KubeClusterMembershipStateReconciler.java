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

package com.netflix.titus.ext.kube.clustermembership.connector;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeLeaderElectionActions;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeRegistrationActions;
import reactor.core.publisher.Mono;

class KubeClusterMembershipStateReconciler implements Function<KubeClusterState, List<Mono<Function<KubeClusterState, KubeClusterState>>>> {

    private final KubeConnectorConfiguration configuration;

    private final Clock clock;
    private final TitusRuntime titusRuntime;
    private final KubeContext context;

    KubeClusterMembershipStateReconciler(KubeContext context, KubeConnectorConfiguration configuration) {
        this.context = context;
        this.configuration = configuration;
        this.titusRuntime = context.getTitusRuntime();
        this.clock = titusRuntime.getClock();
    }

    @Override
    public List<Mono<Function<KubeClusterState, KubeClusterState>>> apply(KubeClusterState kubeClusterState) {

        // Check leader election process.

        boolean inLeaderElectionProcess = context.getKubeLeaderElectionExecutor().isInLeaderElectionProcess();
        if (kubeClusterState.isInLeaderElectionProcess()) {
            if (!inLeaderElectionProcess) {
                return Collections.singletonList(KubeLeaderElectionActions.createJoinLeadershipGroupAction(context));
            }
        } else {
            if (inLeaderElectionProcess) {
                return Collections.singletonList(KubeLeaderElectionActions.createLeaveLeadershipGroupAction(context, false));
            }
        }

        // Refresh registration data so it does not look stale to other cluster members

        if (kubeClusterState.isRegistered() && clock.isPast(kubeClusterState.getLocalMemberRevision().getTimestamp() + configuration.getReRegistrationIntervalMs())) {
            return Collections.singletonList(KubeRegistrationActions.register(context, kubeClusterState, clusterMember ->
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
