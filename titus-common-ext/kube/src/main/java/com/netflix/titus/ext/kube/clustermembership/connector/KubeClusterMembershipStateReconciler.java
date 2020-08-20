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
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeLeaderElectionActions;
import com.netflix.titus.ext.kube.clustermembership.connector.action.KubeRegistrationActions;
import reactor.core.publisher.Mono;

class KubeClusterMembershipStateReconciler implements Function<KubeClusterState, List<Mono<Function<KubeClusterState, KubeClusterState>>>> {

    private final KubeClusterMembershipConfiguration configuration;

    private final Clock clock;
    private final TitusRuntime titusRuntime;
    private final KubeContext context;

    private final Id reconcilerMetricId;
    private final Registry registry;
    private final Random random;

    KubeClusterMembershipStateReconciler(KubeContext context,
                                         KubeClusterMembershipConfiguration configuration) {
        this.context = context;
        this.configuration = configuration;
        this.titusRuntime = context.getTitusRuntime();
        this.clock = titusRuntime.getClock();
        this.random = new Random(System.nanoTime());

        this.registry = titusRuntime.getRegistry();
        this.reconcilerMetricId = registry.createId(KubeMetrics.KUBE_METRIC_ROOT + "reconciler");
    }

    @Override
    public List<Mono<Function<KubeClusterState, KubeClusterState>>> apply(KubeClusterState kubeClusterState) {

        // Check leader election process.
        boolean inLeaderElectionProcess = context.getKubeLeaderElectionExecutor().isInLeaderElectionProcess();
        if (kubeClusterState.isInLeaderElectionProcess()) {
            if (!inLeaderElectionProcess) {
                return Collections.singletonList(withMetrics(
                        "joinLeadershipGroup",
                        KubeLeaderElectionActions.createJoinLeadershipGroupAction(context)
                ));
            }
        } else {
            if (inLeaderElectionProcess) {
                return Collections.singletonList(withMetrics(
                        "leaveLeadershipGroup",
                        KubeLeaderElectionActions.createLeaveLeadershipGroupAction(context, false)
                ));
            }
        }

        // Refresh registration data so it does not look stale to other cluster members
        if (kubeClusterState.isRegistered() && clock.isPast(kubeClusterState.getLocalMemberRevision().getTimestamp() + configuration.getReRegistrationIntervalMs())) {
            Mono<Function<KubeClusterState, KubeClusterState>> action = KubeRegistrationActions.registerLocal(context, kubeClusterState, clusterMember ->
                    ClusterMembershipRevision.<ClusterMember>newBuilder()
                            .withCurrent(clusterMember)
                            .withTimestamp(titusRuntime.getClock().wallTime())
                            .withCode("reregistered")
                            .withMessage("Registration update")
                            .build());
            return Collections.singletonList(withMetrics("register", action));
        }

        // Check for stale data
        long cleanupThreshold = applyJitter(configuration.getRegistrationCleanupThresholdMs(), 30);
        Optional<ClusterMembershipRevision<ClusterMember>> staleRegistration = kubeClusterState.getStaleClusterMemberSiblings().values().stream()
                .filter(s -> clock.isPast(s.getTimestamp() + cleanupThreshold))
                .findFirst();
        if (staleRegistration.isPresent()) {
            String staleMemberId = staleRegistration.get().getCurrent().getMemberId();
            return Collections.singletonList(withMetrics("removeStaleMember", KubeRegistrationActions.removeStaleRegistration(context, staleMemberId)));
        }

        return Collections.emptyList();
    }

    private <T> Mono<T> withMetrics(String name, Mono<T> action) {
        return action
                .doOnSuccess(v ->
                        registry.counter(reconcilerMetricId.withTag("action", name)).increment())
                .doOnError(error ->
                        registry.counter(reconcilerMetricId
                                .withTag("action", name)
                                .withTag("error", error.getClass().getSimpleName())
                        ).increment());
    }

    private long applyJitter(long value, int percent) {
        return ((100 + random.nextInt(percent)) * value) / 100;
    }
}
