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

import com.netflix.titus.ext.kube.clustermembership.connector.KubeClusterState;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeContext;
import reactor.core.publisher.Mono;

public class KubeLeaderElectionActions {

    public static Mono<Function<KubeClusterState, KubeClusterState>> createJoinLeadershipGroupAction(KubeContext context) {
        return Mono.just(currentState -> {
            if (currentState.isInLeaderElectionProcess()) {
                return currentState;
            }

            context.getKubeLeaderElectionExecutor().joinLeaderElectionProcess();
            return currentState.setJoinedLeaderElection();
        });
    }

    public static Mono<Function<KubeClusterState, KubeClusterState>> createLeaveLeadershipGroupAction(KubeContext context, boolean onlyNonLeader) {
        return Mono.just(currentState -> {
            if (!currentState.isInLeaderElectionProcess()) {
                return currentState;
            }

            if (currentState.isLocalLeader() && onlyNonLeader) {
                return currentState;
            }

            context.getKubeLeaderElectionExecutor().leaveLeaderElectionProcess();
            return currentState.setLeaveLeaderElection();
        });
    }
}
