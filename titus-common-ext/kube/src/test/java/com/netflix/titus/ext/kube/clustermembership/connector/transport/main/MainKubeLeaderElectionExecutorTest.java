/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.main;

import com.netflix.titus.ext.kube.clustermembership.connector.KubeLeaderElectionExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.AbstractKubeLeaderElectionExecutorTest;
import com.netflix.titus.testkit.junit.category.RemoteIntegrationTest;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(RemoteIntegrationTest.class)
public class MainKubeLeaderElectionExecutorTest extends AbstractKubeLeaderElectionExecutorTest {

    @ClassRule
    public static final MainKubeExternalResource KUBE_RESOURCE = new MainKubeExternalResource();

    @Override
    protected KubeLeaderElectionExecutor getKubeLeaderElectionExecutor(String memberId) {
        return new MainKubeLeaderElectionExecutor(
                KUBE_RESOURCE.getClient(),
                "default",
                clusterName,
                LEASE_DURATION,
                memberId,
                titusRuntime
        );
    }
}