/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.scheduler.autoscale;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.master.MetricConstants;

class AutoScaleControllerMetrics {

    private static final String ROOT = MetricConstants.METRIC_SCHEDULING_SERVICE + "autoscale.requests";

    private final Registry registry;

    private enum ScaleUpState {
        UnknownInstanceGroup,
        NotAllowed,
        DesiredSizeUnchanged,
        Succeeded,
        Failed
    }

    private enum ScaleDownState {
        UnknownInstanceGroup,
        NotAllowed,
        NoInstanceToTerminate,
        Succeeded,
        PartialFailure,
        Failed
    }

    AutoScaleControllerMetrics(Registry registry) {
        this.registry = registry;
    }

    void unknownInstanceGroupScaleUp(String instanceGroupName) {
        update(instanceGroupName, ScaleUpState.UnknownInstanceGroup);
    }

    void scaleUpNotAllowed(String instanceGroupName) {
        update(instanceGroupName, ScaleUpState.NotAllowed);
    }

    void scaleUpToExistingDesiredSize(String instanceGroupName) {
        update(instanceGroupName, ScaleUpState.DesiredSizeUnchanged);
    }

    void scaleUpSucceeded(String instanceGroupName) {
        update(instanceGroupName, ScaleUpState.Succeeded);
    }

    void scaleUpFailed(String instanceGroupName, Throwable e) {
        registry.counter(ROOT,
                "id", instanceGroupName,
                "action", "ScaleUp",
                "state", ScaleUpState.Failed.name(),
                "error", e.getClass().getSimpleName()
        ).increment();
    }

    void unknownInstanceGroupScaleDown(String instanceGroupName) {
        update(instanceGroupName, ScaleDownState.UnknownInstanceGroup);
    }

    void scaleDownNotAllowed(String instanceGroupName) {
        update(instanceGroupName, ScaleDownState.NotAllowed);
    }

    void scaleDownFinished(String instanceGroupName, int terminateOk, int terminateError) {
        if (terminateOk > 0 && terminateError == 0) {
            update(instanceGroupName, ScaleDownState.Succeeded);
        } else if (terminateOk == 0 && terminateError > 0) {
            update(instanceGroupName, ScaleDownState.Failed);
        } else if (terminateOk > 0 && terminateError > 0) {
            update(instanceGroupName, ScaleDownState.PartialFailure);
        }
    }

    void scaleDownFailed(String instanceGroupName, Throwable e) {
        registry.counter(ROOT,
                "id", instanceGroupName,
                "action", "ScaleDown",
                "state", ScaleUpState.Failed.name(),
                "error", e.getClass().getSimpleName()
        ).increment();
    }

    void noInstancesToScaleDown(AgentInstanceGroup instanceGroup) {
    }

    private void update(String instanceGroupName, ScaleUpState scaleUpState) {
        registry.counter(ROOT,
                "id", instanceGroupName,
                "action", "ScaleUp",
                "state", scaleUpState.name()
        ).increment();
    }

    private void update(String instanceGroupName, ScaleDownState scaleDownState) {
        registry.counter(ROOT,
                "id", instanceGroupName,
                "action", "ScaleDown",
                "state", scaleDownState.name()
        ).increment();
    }
}
