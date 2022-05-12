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

package com.netflix.titus.master.service.management;

import java.util.Collections;
import java.util.Map;

import com.netflix.titus.api.model.ResourceDimension;

/**
 */
public class CompositeResourceConsumption extends ResourceConsumption {

    private final Map<String, ResourceConsumption> contributors;
    private final ResourceDimension allowedConsumption;
    private final boolean aboveLimit;

    public CompositeResourceConsumption(String consumerName,
                                        ConsumptionLevel consumptionLevel,
                                        ResourceDimension currentConsumption,
                                        ResourceDimension maxConsumption,
                                        ResourceDimension allowedConsumption,
                                        Map<String, Object> attributes,
                                        Map<String, ResourceConsumption> contributors,
                                        boolean aboveLimit) {
        super(consumerName, consumptionLevel, currentConsumption, maxConsumption, attributes);
        this.contributors = contributors;
        this.allowedConsumption = allowedConsumption;
        this.aboveLimit = aboveLimit;
    }

    public Map<String, ResourceConsumption> getContributors() {
        return contributors;
    }

    public ResourceDimension getAllowedConsumption() {
        return allowedConsumption;
    }

    public boolean isAboveLimit() {
        return aboveLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CompositeResourceConsumption that = (CompositeResourceConsumption) o;

        if (aboveLimit != that.aboveLimit) {
            return false;
        }
        if (contributors != null ? !contributors.equals(that.contributors) : that.contributors != null) {
            return false;
        }
        return allowedConsumption != null ? allowedConsumption.equals(that.allowedConsumption) : that.allowedConsumption == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (contributors != null ? contributors.hashCode() : 0);
        result = 31 * result + (allowedConsumption != null ? allowedConsumption.hashCode() : 0);
        result = 31 * result + (aboveLimit ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CompositeResourceConsumption{" +
                "consumerName='" + getConsumerName() + '\'' +
                ", consumptionLevel=" + getConsumptionLevel() +
                ", currentConsumption=" + getCurrentConsumption() +
                ", maxConsumption=" + getMaxConsumption() +
                ", attributes=" + getAttributes() +
                "contributors=" + contributors +
                ", allowedConsumption=" + allowedConsumption +
                ", aboveLimit=" + aboveLimit +
                '}';
    }

    public static CompositeResourceConsumption unused(String consumerName,
                                                      ConsumptionLevel consumptionLevel,
                                                      ResourceDimension allowedConsumption) {
        return new CompositeResourceConsumption(
                consumerName,
                consumptionLevel,
                ResourceDimension.empty(),
                ResourceDimension.empty(),
                allowedConsumption,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false
        );
    }
}
