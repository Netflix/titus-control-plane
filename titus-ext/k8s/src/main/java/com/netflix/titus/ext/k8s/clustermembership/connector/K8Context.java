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

import com.netflix.titus.common.runtime.TitusRuntime;

public class K8Context {

    private final K8MembershipExecutor k8MembershipExecutor;
    private final K8LeaderElectionExecutor k8LeaderElectionExecutor;
    private final TitusRuntime titusRuntime;

    K8Context(K8MembershipExecutor k8MembershipExecutor,
              K8LeaderElectionExecutor k8LeaderElectionExecutor,
              TitusRuntime titusRuntime) {
        this.k8MembershipExecutor = k8MembershipExecutor;
        this.k8LeaderElectionExecutor = k8LeaderElectionExecutor;
        this.titusRuntime = titusRuntime;
    }

    public K8MembershipExecutor getK8MembershipExecutor() {
        return k8MembershipExecutor;
    }

    public K8LeaderElectionExecutor getK8LeaderElectionExecutor() {
        return k8LeaderElectionExecutor;
    }

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }
}
