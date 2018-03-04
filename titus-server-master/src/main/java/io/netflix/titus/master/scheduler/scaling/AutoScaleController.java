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

package io.netflix.titus.master.scheduler.scaling;

import java.util.Set;

import io.netflix.titus.api.model.event.AutoScaleEvent;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

public interface AutoScaleController {

    /**
     * Scales up instance group by adding the specified number of instances.
     */
    void handleScaleUpAction(String instanceGroupName, int scaleUpCount);

    /**
     * Removes agent instances with the given hostnames. Returns a pair of two sets where the first one contains
     * host names of the instances for which terminate request was executed. The second set contains host names
     * which are either unknown, or due to other restrictions/constraints their termination was not allowed.
     */
    Pair<Set<String>, Set<String>> handleScaleDownAction(String instanceGroupName, Set<String> instanceIds);

    /**
     * Emits an event for each successful or unsuccessful autoscale action.
     */
    Observable<AutoScaleEvent> events();
}
