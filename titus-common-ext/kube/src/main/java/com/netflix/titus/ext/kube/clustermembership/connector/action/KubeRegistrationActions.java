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

package com.netflix.titus.ext.kube.clustermembership.connector.action;

import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeClusterState;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeContext;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMembershipExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.KubeUtils;
import reactor.core.publisher.Mono;

public class KubeRegistrationActions {

    public static Mono<Function<KubeClusterState, KubeClusterState>> registerLocal(KubeContext context,
                                                                                   KubeClusterState kubeClusterState,
                                                                                   Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {

        ClusterMember localMember = kubeClusterState.getLocalMemberRevision().getCurrent();
        ClusterMembershipRevision<ClusterMember> newRevision = setRegistrationStatus(selfUpdate.apply(localMember), true);

        KubeMembershipExecutor membershipExecutor = context.getKubeMembershipExecutor();

        Mono<ClusterMembershipRevision<ClusterMember>> monoAction;
        if (kubeClusterState.isRegistered()) {
            monoAction = membershipExecutor
                    .updateLocal(newRevision)
                    .onErrorResume(e -> {
                        if (!KubeUtils.is4xx(e)) {
                            return Mono.error(e);
                        }
                        int status = KubeUtils.getHttpStatusCode(e);
                        if (status == 404) {
                            return membershipExecutor.createLocal(newRevision);
                        }
                        // Bad or stale data record. Remove it first and than register.
                        return membershipExecutor
                                .removeMember(newRevision.getCurrent().getMemberId())
                                .then(membershipExecutor.createLocal(newRevision));
                    });
        } else {
            monoAction = membershipExecutor
                    .createLocal(newRevision)
                    .onErrorResume(e -> {
                        if (!KubeUtils.is4xx(e)) {
                            return Mono.error(e);
                        }
                        // Bad or stale data record. Remove it first and than register.
                        return membershipExecutor
                                .removeMember(newRevision.getCurrent().getMemberId())
                                .then(membershipExecutor.createLocal(newRevision));
                    });
        }

        return monoAction
                .onErrorMap(KubeUtils::toConnectorException)
                .map(update -> currentState -> currentState.setLocalClusterMemberRevision(update));
    }

    public static Mono<Function<KubeClusterState, KubeClusterState>> unregisterLocal(KubeContext context,
                                                                                     KubeClusterState kubeClusterState,
                                                                                     Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        if (!kubeClusterState.isRegistered()) {
            return Mono.just(Function.identity());
        }

        ClusterMember localMember = kubeClusterState.getLocalMemberRevision().getCurrent();
        ClusterMembershipRevision<ClusterMember> newRevision = setRegistrationStatus(selfUpdate.apply(localMember), false);

        Mono monoAction = context.getKubeMembershipExecutor().removeMember(kubeClusterState.getLocalMemberRevision().getCurrent().getMemberId());
        return ((Mono<Function<KubeClusterState, KubeClusterState>>) monoAction)
                .onErrorMap(KubeUtils::toConnectorException)
                .thenReturn(currentState -> currentState.setLocalClusterMemberRevision(newRevision));
    }

    public static Mono<Function<KubeClusterState, KubeClusterState>> removeStaleRegistration(KubeContext context, String memberId) {
        return context.getKubeMembershipExecutor().removeMember(memberId)
                .onErrorMap(KubeUtils::toConnectorException)
                .thenReturn(s -> s.removeStaleMember(memberId));
    }

    private static ClusterMembershipRevision<ClusterMember> setRegistrationStatus(ClusterMembershipRevision<ClusterMember> revision, boolean registered) {
        return revision.toBuilder()
                .withCurrent(revision.getCurrent().toBuilder().withRegistered(registered).build())
                .build();
    }
}
