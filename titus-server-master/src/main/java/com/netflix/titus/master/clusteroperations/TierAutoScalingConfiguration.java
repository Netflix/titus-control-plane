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

package com.netflix.titus.master.clusteroperations;

public class TierAutoScalingConfiguration {

    private final String primaryInstanceType;
    private final Long scaleUpCoolDownMs;
    private final Long scaleDownCoolDownMs;
    private final Integer minIdle;
    private final Integer maxIdle;
    private final Long taskSloMs;
    private final Long idleInstanceGracePeriodMs;

    public TierAutoScalingConfiguration(String primaryInstanceType,
                                        Long scaleUpCoolDownMs,
                                        Long scaleDownCoolDownMs,
                                        Integer minIdle,
                                        Integer maxIdle,
                                        Long taskSloMs,
                                        Long idleInstanceGracePeriodMs) {
        this.primaryInstanceType = primaryInstanceType;
        this.scaleUpCoolDownMs = scaleUpCoolDownMs;
        this.scaleDownCoolDownMs = scaleDownCoolDownMs;
        this.minIdle = minIdle;
        this.maxIdle = maxIdle;
        this.taskSloMs = taskSloMs;
        this.idleInstanceGracePeriodMs = idleInstanceGracePeriodMs;
    }

    public String getPrimaryInstanceType() {
        return primaryInstanceType;
    }

    public Long getScaleUpCoolDownMs() {
        return scaleUpCoolDownMs;
    }

    public Long getScaleDownCoolDownMs() {
        return scaleDownCoolDownMs;
    }

    public Integer getMinIdle() {
        return minIdle;
    }

    public Integer getMaxIdle() {
        return maxIdle;
    }

    public Long getTaskSloMs() {
        return taskSloMs;
    }

    public Long getIdleInstanceGracePeriodMs() {
        return idleInstanceGracePeriodMs;
    }
}
