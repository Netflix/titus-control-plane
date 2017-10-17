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

package io.netflix.titus.master.scheduler;

import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.VirtualMachineLease;

public class FenzoAutoScaleRuleWrapper implements AutoScaleRule {

    private final io.netflix.titus.api.agent.model.AutoScaleRule titusAutoScaleRule;

    public FenzoAutoScaleRuleWrapper(io.netflix.titus.api.agent.model.AutoScaleRule titusAutoScaleRule) {
        this.titusAutoScaleRule = titusAutoScaleRule;
    }

    @Override
    public String getRuleName() {
        return titusAutoScaleRule.getInstanceGroupId();
    }

    @Override
    public int getMinIdleHostsToKeep() {
        return titusAutoScaleRule.getMinIdleToKeep();
    }

    @Override
    public int getMaxIdleHostsToKeep() {
        return titusAutoScaleRule.getMaxIdleToKeep();
    }

    @Override
    public long getCoolDownSecs() {
        return titusAutoScaleRule.getCoolDownSec();
    }

    @Override
    public boolean idleMachineTooSmall(VirtualMachineLease lease) {
        return lease == null;
    }

    @Override
    public int getShortfallAdjustedAgents(int numberOfAgents) {
        if (titusAutoScaleRule.getShortfallAdjustingFactor() <= 0) {
            return numberOfAgents;
        }
        return (int) Math.ceil(numberOfAgents / (double) titusAutoScaleRule.getShortfallAdjustingFactor());
    }
}
