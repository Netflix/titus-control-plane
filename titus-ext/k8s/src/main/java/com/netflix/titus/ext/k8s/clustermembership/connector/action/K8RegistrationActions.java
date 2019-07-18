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

package com.netflix.titus.ext.k8s.clustermembership.connector.action;

import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.ext.k8s.clustermembership.connector.K8ClusterState;
import com.netflix.titus.ext.k8s.clustermembership.connector.K8Context;
import com.netflix.titus.ext.k8s.clustermembership.connector.K8MembershipExecutor;
import reactor.core.publisher.Mono;

public class K8RegistrationActions {

    public static Mono<Function<K8ClusterState, K8ClusterState>> register(K8Context context,
                                                                          K8ClusterState k8ClusterState,
                                                                          Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {

        ClusterMember localMember = k8ClusterState.getLocalMemberRevision().getCurrent();
        ClusterMembershipRevision<ClusterMember> newRevision = selfUpdate.apply(localMember);

        K8MembershipExecutor membershipExecutor = context.getK8MembershipExecutor();

        Mono<ClusterMembershipRevision<ClusterMember>> monoAction;
        if (k8ClusterState.isRegistered()) {
            monoAction = membershipExecutor
                    .updateLocal(newRevision)
                    .onErrorResume(e -> K8ActionsUtil.is4xx(e) ? membershipExecutor.createLocal(newRevision) : Mono.error(e));
        } else {
            monoAction = membershipExecutor
                    .createLocal(newRevision)
                    .onErrorResume(e -> K8ActionsUtil.is4xx(e) ? membershipExecutor.updateLocal(newRevision) : Mono.error(e));
        }

        return monoAction.map(update -> currentState -> currentState.setLocalClusterMemberRevision(update));
    }

    public static Mono<Function<K8ClusterState, K8ClusterState>> unregister(K8Context context, K8ClusterState k8ClusterState) {
        if (!k8ClusterState.isRegistered()) {
            return Mono.just(Function.identity());
        }

        return context.getK8MembershipExecutor()
                .removeLocal(k8ClusterState.getLocalMemberRevision().getCurrent().getMemberId())
                .thenReturn(K8ClusterState::setUnregistered);
    }
}
