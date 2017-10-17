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

package io.netflix.titus.master.service.management;

import java.util.Map;

import io.netflix.titus.api.model.ResourceDimension;

/**
 * Hierarchical resource consumption, which is system / tiers / capacity groups / applications.
 */
public class ResourceConsumption {

    public static final String SYSTEM_CONSUMER = "system";

    public enum ConsumptionLevel {System, Tier, CapacityGroup, Application}

    private final String consumerName;
    private final ConsumptionLevel consumptionLevel;
    private final ResourceDimension currentConsumption;
    private final ResourceDimension maxConsumption;
    private final Map<String, Object> attributes;

    public ResourceConsumption(String consumerName, ConsumptionLevel consumptionLevel, ResourceDimension currentConsumption, ResourceDimension maxConsumption, Map<String, Object> attributes) {
        this.consumerName = consumerName;
        this.consumptionLevel = consumptionLevel;
        this.currentConsumption = currentConsumption;
        this.maxConsumption = maxConsumption;
        this.attributes = attributes;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public ConsumptionLevel getConsumptionLevel() {
        return consumptionLevel;
    }

    public ResourceDimension getCurrentConsumption() {
        return currentConsumption;
    }

    public ResourceDimension getMaxConsumption() {
        return maxConsumption;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResourceConsumption that = (ResourceConsumption) o;

        if (consumerName != null ? !consumerName.equals(that.consumerName) : that.consumerName != null) {
            return false;
        }
        if (consumptionLevel != that.consumptionLevel) {
            return false;
        }
        if (currentConsumption != null ? !currentConsumption.equals(that.currentConsumption) : that.currentConsumption != null) {
            return false;
        }
        if (maxConsumption != null ? !maxConsumption.equals(that.maxConsumption) : that.maxConsumption != null) {
            return false;
        }
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;

    }

    @Override
    public int hashCode() {
        int result = consumerName != null ? consumerName.hashCode() : 0;
        result = 31 * result + (consumptionLevel != null ? consumptionLevel.hashCode() : 0);
        result = 31 * result + (currentConsumption != null ? currentConsumption.hashCode() : 0);
        result = 31 * result + (maxConsumption != null ? maxConsumption.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ResourceConsumption{" +
                "consumerName='" + consumerName + '\'' +
                ", consumptionLevel=" + consumptionLevel +
                ", currentConsumption=" + currentConsumption +
                ", maxConsumption=" + maxConsumption +
                ", attributes=" + attributes +
                '}';
    }
}
