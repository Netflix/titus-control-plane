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

import com.netflix.titus.ext.k8s.clustermembership.connector.K8ClusterState;
import com.netflix.titus.ext.k8s.clustermembership.connector.K8Context;
import reactor.core.publisher.Mono;

public class K8LeaderElectionActions {

    public static Mono<Function<K8ClusterState, K8ClusterState>> createJoinLeadershipGroupAction(K8Context context) {
        return Mono.just(currentState -> {
            if (currentState.isInLeaderElectionProcess()) {
                return currentState;
            }

            context.getK8LeaderElectionExecutor().joinLeaderElectionProcess();
            return currentState.setJoinedLeaderElection();
        });
    }

    public static Mono<Function<K8ClusterState, K8ClusterState>> createLeaveLeadershipGroupAction(K8Context context, boolean onlyNonLeader) {
        return Mono.just(currentState -> {
            if (!currentState.isInLeaderElectionProcess()) {
                return currentState;
            }

            if (currentState.isLocalLeader() && onlyNonLeader) {
                return currentState;
            }

            context.getK8LeaderElectionExecutor().leaveLeaderElectionProcess();
            return currentState.setLeaveLeaderElection();
        });
    }
}
