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

package io.netflix.titus.master.scheduler.constraint;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;

public class CompositeGlobalConstraintEvaluator implements GlobalConstraintEvaluator {

    private final List<GlobalConstraintEvaluator> evaluators;
    private final String name;

    public CompositeGlobalConstraintEvaluator(List<GlobalConstraintEvaluator> evaluators) {
        Preconditions.checkArgument(!evaluators.isEmpty());

        this.evaluators = evaluators;
        this.name = "CompositeOf(" + String.join(",", evaluators.stream().map(ConstraintEvaluator::getName).collect(Collectors.toList())) + ')';
    }


    @Override
    public void prepare() {
        evaluators.forEach(GlobalConstraintEvaluator::prepare);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        Result result = null;
        for (GlobalConstraintEvaluator evaluator : evaluators) {
            result = evaluator.evaluate(taskRequest, targetVM, taskTrackerState);
            if (!result.isSuccessful()) {
                return result;
            }
        }
        return result;
    }
}
