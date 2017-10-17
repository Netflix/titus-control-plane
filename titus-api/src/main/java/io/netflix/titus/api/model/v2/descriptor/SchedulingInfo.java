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

package io.netflix.titus.api.model.v2.descriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;


public class SchedulingInfo {

    private Map<Integer, StageSchedulingInfo> stages = new HashMap<>();

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public SchedulingInfo(@JsonProperty("stages") Map<Integer, StageSchedulingInfo> stages) {
        this.stages = stages;
    }

    @JsonIgnore
    SchedulingInfo(Builder builder) {
        stages.putAll(builder.builderStages);
    }

    public Map<Integer, StageSchedulingInfo> getStages() {
        return stages;
    }

    public StageSchedulingInfo forStage(int stageNum) {
        return stages.get(stageNum);
    }

    public static class Builder {

        private Map<Integer, StageSchedulingInfo> builderStages = new HashMap<>();
        private Integer currentStage = 1;
        private int numberOfStages;

        public Builder numberOfStages(int numberOfStages) {
            this.numberOfStages = numberOfStages;
            return this;
        }

        public Builder singleWorkerStageWithConstraints(MachineDefinition machineDefinition,
                                                        List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints) {
            builderStages.put(currentStage, new StageSchedulingInfo(1, machineDefinition, hardConstraints,
                    softConstraints, null, false, null, false));
            currentStage++;
            return this;
        }

        public Builder singleWorkerStage(MachineDefinition machineDefinition) {
            builderStages.put(currentStage, new StageSchedulingInfo(1, machineDefinition, null,
                    null, null, false, null, false));
            currentStage++;
            return this;
        }

        public Builder multiWorkerScalableStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                               List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints,
                                                               StageScalingPolicy scalingPolicy) {
            StageScalingPolicy ssp = new StageScalingPolicy(currentStage, scalingPolicy.getMin(), scalingPolicy.getMax(),
                    numberOfWorkers, scalingPolicy.getIncrement(), scalingPolicy.getDecrement(),
                    scalingPolicy.getCoolDownSecs(), scalingPolicy.getStrategies());
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    hardConstraints, softConstraints, null, false, ssp, ssp.isEnabled()));
            currentStage++;
            return this;
        }

        public Builder multiWorkerStageWithConstraints(int numberOfWorkers, MachineDefinition machineDefinition,
                                                       List<JobConstraints> hardConstraints, List<JobConstraints> softConstraints) {
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    hardConstraints, softConstraints, null, false, null, false));
            currentStage++;
            return this;
        }

        public Builder multiWorkerStage(int numberOfWorkers, MachineDefinition machineDefinition) {
            builderStages.put(currentStage, new StageSchedulingInfo(numberOfWorkers, machineDefinition,
                    null, null, null, false, null, false));
            currentStage++;
            return this;
        }

        public SchedulingInfo build() {
            if (numberOfStages == 0) {
                throw new IllegalArgumentException("Number of stages is 0, must be specified using builder.");
            }
            if (numberOfStages != builderStages.size()) {
                throw new IllegalArgumentException("Missing scheduling information, number of stages: " + numberOfStages
                        + " configured stages: " + builderStages.size());
            }
            return new SchedulingInfo(this);
        }
    }
}
