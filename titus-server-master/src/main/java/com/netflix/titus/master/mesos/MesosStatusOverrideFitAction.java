/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.mesos;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.framework.fit.AbstractFitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitInjection;
import org.apache.mesos.Protos;

public class MesosStatusOverrideFitAction extends AbstractFitAction {

    public static final FitActionDescriptor DESCRIPTOR = new FitActionDescriptor(
            "mesosTaskStatusOverride",
            "Override task status returned by Mesos",
            ImmutableMap.of(
                    "onTaskState", "Task state to intercept (ignored if not defined)",
                    "onReasonCode", "Reason code to intercept (ignored if not defined)",
                    "override", "Reason message to inject (defaults to 'FIT error')"
            )
    );

    private final Optional<Protos.TaskState> taskState;
    private final Optional<Protos.TaskStatus.Reason> reasonCode;
    private final String override;

    public MesosStatusOverrideFitAction(String id, Map<String, String> properties, FitInjection injection) {
        super(id, DESCRIPTOR, properties, injection);

        this.taskState = Optional.ofNullable(properties.get("onTaskState")).map(Protos.TaskState::valueOf);
        this.reasonCode = Optional.ofNullable(properties.get("onReasonCode")).map(Protos.TaskStatus.Reason::valueOf);
        this.override = properties.getOrDefault("override", "FIT error");
    }

    @Override
    public <T> T afterImmediate(String injectionPoint, T result) {
        if (!(result instanceof Protos.TaskStatus)) {
            return result;
        }
        Protos.TaskStatus status = (Protos.TaskStatus) result;

        boolean taskMatches = taskState.map(expected -> status.getState() == expected).orElse(false);
        boolean reasonMatches = reasonCode.map(expected -> status.getReason() == expected).orElse(false);
        if (taskMatches || reasonMatches) {
            return (T) status.toBuilder().setMessage(override).build();
        }
        return result;
    }
}
