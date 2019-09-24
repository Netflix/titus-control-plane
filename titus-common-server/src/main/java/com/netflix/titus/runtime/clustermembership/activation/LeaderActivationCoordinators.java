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

package com.netflix.titus.runtime.clustermembership.activation;

import java.util.List;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.runtime.SystemAbortEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.SystemExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LeaderActivationCoordinators {

    private static final Logger logger = LoggerFactory.getLogger(LeaderActivationCoordinators.class);

    private LeaderActivationCoordinators() {
    }

    public static LeaderActivationCoordinator coordinatorWithLoggingCallback(LeaderActivationConfiguration configuration,
                                                                             List<LeaderActivationListener> services,
                                                                             ClusterMembershipService membershipService,
                                                                             TitusRuntime titusRuntime) {
        return new LeaderActivationCoordinator(
                configuration,
                services,
                error -> logger.info(error.getMessage()),
                membershipService,
                titusRuntime
        );
    }

    public static LeaderActivationCoordinator coordinatorWithSystemExitCallback(LeaderActivationConfiguration configuration,
                                                                                List<LeaderActivationListener> services,
                                                                                ClusterMembershipService membershipService,
                                                                                TitusRuntime titusRuntime) {

        return new LeaderActivationCoordinator(
                configuration,
                services,
                e -> {
                    titusRuntime.beforeAbort(SystemAbortEvent.newBuilder()
                            .withFailureId("activationError")
                            .withFailureType(SystemAbortEvent.FailureType.Nonrecoverable)
                            .withReason(e.getMessage())
                            .withTimestamp(titusRuntime.getClock().wallTime())
                            .build()
                    );
                    SystemExt.forcedProcessExit(-1);
                },
                membershipService,
                titusRuntime
        );
    }
}
