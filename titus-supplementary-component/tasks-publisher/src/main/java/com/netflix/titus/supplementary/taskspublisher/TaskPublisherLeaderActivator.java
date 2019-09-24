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

package com.netflix.titus.supplementary.taskspublisher;

import java.util.Collections;
import java.util.List;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationConfiguration;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinator;
import com.netflix.titus.supplementary.taskspublisher.es.EsPublisher;

import static com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinators.coordinatorWithLoggingCallback;
import static com.netflix.titus.runtime.clustermembership.activation.LeaderActivationCoordinators.coordinatorWithSystemExitCallback;

@Singleton
public class TaskPublisherLeaderActivator {

    private final LeaderActivationCoordinator coordinator;

    @Inject
    public TaskPublisherLeaderActivator(LeaderActivationConfiguration configuration,
                                        EsPublisher esPublisher,
                                        ClusterMembershipService membershipService,
                                        TitusRuntime titusRuntime) {
        List<LeaderActivationListener> services = Collections.singletonList(esPublisher);
        this.coordinator = configuration.isSystemExitOnLeadershipLost()
                ? coordinatorWithSystemExitCallback(configuration, services, membershipService, titusRuntime)
                : coordinatorWithLoggingCallback(configuration, services, membershipService, titusRuntime);
    }

    @PreDestroy
    public void shutdown() {
        coordinator.shutdown();
    }
}
