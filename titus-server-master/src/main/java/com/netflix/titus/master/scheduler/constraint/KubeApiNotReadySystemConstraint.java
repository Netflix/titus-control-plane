/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.scheduler.constraint;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;

@Singleton
public class KubeApiNotReadySystemConstraint implements SystemConstraint {

    public static final String NAME = KubeApiNotReadySystemConstraint.class.getSimpleName();

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "Pod informer not ready");

    private final KubeApiFacade kubeApiFacade;
    private final MesosConfiguration mesosConfiguration;

    @Inject
    public KubeApiNotReadySystemConstraint(KubeApiFacade kubeApiFacade,
                                           MesosConfiguration mesosConfiguration) {
        this.kubeApiFacade = kubeApiFacade;
        this.mesosConfiguration = mesosConfiguration;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (mesosConfiguration.isKubeApiServerIntegrationEnabled()) {
            return kubeApiFacade.isReadyForScheduling() ? VALID : INVALID;
        }
        return VALID;
    }

    public static boolean isKubeApiNotReadySystemConstraintReason(String reason) {
        return reason != null && INVALID.getFailureReason().contains(reason);
    }
}
