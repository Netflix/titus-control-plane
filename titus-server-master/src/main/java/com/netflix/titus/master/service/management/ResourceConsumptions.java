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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.master.model.ResourceDimensions;

import static java.util.Arrays.asList;

public final class ResourceConsumptions {

    private ResourceConsumptions() {
    }

    /**
     * Find {@link ResourceConsumption} instance in the consumption hierarchy.
     */
    public static Optional<ResourceConsumption> findConsumption(ResourceConsumption parent, String... path) {
        ResourceConsumption current = parent;
        for (String segment : path) {
            if (!(current instanceof CompositeResourceConsumption)) {
                return Optional.empty();
            }
            current = ((CompositeResourceConsumption) current).getContributors().get(segment);
            if (current == null) {
                return Optional.empty();
            }
        }
        return Optional.of(current);
    }

    /**
     * Find all {@link ResourceConsumption} instances at a specific consumption level.
     */
    public static Map<String, ResourceConsumption> groupBy(CompositeResourceConsumption parent,
                                                           ResourceConsumption.ConsumptionLevel level) {
        Preconditions.checkArgument(parent.getConsumptionLevel().ordinal() <= level.ordinal());

        if (parent.getConsumptionLevel() == level) {
            return Collections.singletonMap(parent.getConsumerName(), parent);
        }

        // If this is the Application level, the next one needs to be the InstanceType level, which is the last one
        if (parent.getConsumptionLevel() == ResourceConsumption.ConsumptionLevel.Application) {
            Preconditions.checkArgument(level == ResourceConsumption.ConsumptionLevel.InstanceType);
            return parent.getContributors();
        }

        Map<String, ResourceConsumption> result = new HashMap<>();
        for (ResourceConsumption nested : parent.getContributors().values()) {
            result.putAll(groupBy((CompositeResourceConsumption) nested, level));
        }
        return result;
    }

    /**
     * Add a collection of {@link ResourceConsumption} values of the same kind (the same consumption name and level).
     *
     * @return aggregated value of the same type as the input
     */
    public static <C extends ResourceConsumption> C add(Collection<C> consumptions) {
        verifySameKind(consumptions);

        List<C> consumptionList = new ArrayList<>(consumptions);
        if (consumptionList.size() == 1) {
            return consumptionList.get(0);
        }

        ResourceDimension currentUsage = addCurrentConsumptions(consumptionList);
        ResourceDimension maxUsage = addMaxConsumptions(consumptionList);

        Map<String, Object> mergedAttrs = mergeAttributesOf(consumptions);

        C first = consumptionList.get(0);
        if (first instanceof CompositeResourceConsumption) {
            Map<String, ResourceConsumption> mergedContributors = new HashMap<>();
            consumptionList.stream()
                    .map(CompositeResourceConsumption.class::cast)
                    .flatMap(c -> c.getContributors().entrySet().stream())
                    .forEach(entry -> mergedContributors.compute(entry.getKey(), (name, current) ->
                            current == null ? entry.getValue() : ResourceConsumptions.add(current, entry.getValue())
                    ));

            ResourceDimension allowedUsage = addAllowedConsumptions((Collection<CompositeResourceConsumption>) consumptionList);

            return (C) new CompositeResourceConsumption(
                    first.getConsumerName(),
                    first.getConsumptionLevel(),
                    currentUsage,
                    maxUsage,
                    allowedUsage,
                    mergedAttrs,
                    mergedContributors,
                    !ResourceDimensions.isBigger(allowedUsage, maxUsage)
            );
        }

        return (C) new ResourceConsumption(
                first.getConsumerName(),
                first.getConsumptionLevel(),
                currentUsage,
                maxUsage,
                mergedAttrs
        );
    }

    /**
     * See {@link #add(Collection)} for description.
     */
    public static ResourceConsumption add(ResourceConsumption... consumptions) {
        return add(asList(consumptions));
    }

    /**
     * Create a parent {@link CompositeResourceConsumption} instance from the provided consumption collection.
     */
    public static CompositeResourceConsumption aggregate(String parentName,
                                                         ResourceConsumption.ConsumptionLevel parentLevel,
                                                         Collection<? extends ResourceConsumption> consumptions,
                                                         ResourceDimension allowedUsage) {

        Map<String, ResourceConsumption> contributors = new HashMap<>();
        consumptions.forEach(c -> contributors.put(c.getConsumerName(), c));

        ResourceDimension currentUsage = addCurrentConsumptions(consumptions);
        ResourceDimension maxUsage = addMaxConsumptions(consumptions);

        Map<String, Object> mergedAttrs = mergeAttributesOf(consumptions);

        return new CompositeResourceConsumption(
                parentName,
                parentLevel,
                currentUsage,
                maxUsage,
                allowedUsage, mergedAttrs,
                contributors,
                !ResourceDimensions.isBigger(allowedUsage, maxUsage)
        );
    }

    public static CompositeResourceConsumption aggregate(String parentName,
                                                         ResourceConsumption.ConsumptionLevel parentLevel,
                                                         Collection<CompositeResourceConsumption> consumptions) {
        ResourceDimension allowedUsage = addAllowedConsumptions(consumptions);
        return aggregate(parentName, parentLevel, consumptions, allowedUsage);
    }

    public static <C extends ResourceConsumption> ResourceDimension addCurrentConsumptions(Collection<C> consumptions) {
        List<ResourceDimension> all = consumptions.stream().map(ResourceConsumption::getCurrentConsumption).collect(Collectors.toList());
        return ResourceDimensions.add(all);
    }

    public static <C extends ResourceConsumption> ResourceDimension addMaxConsumptions(Collection<C> consumptions) {
        List<ResourceDimension> all = consumptions.stream().map(ResourceConsumption::getMaxConsumption).collect(Collectors.toList());
        return ResourceDimensions.add(all);
    }

    public static ResourceDimension addAllowedConsumptions(Collection<CompositeResourceConsumption> consumptions) {
        List<ResourceDimension> all = consumptions.stream().map(CompositeResourceConsumption::getAllowedConsumption).collect(Collectors.toList());
        return ResourceDimensions.add(all);
    }

    public static Map<String, Object> mergeAttributes(Collection<Map<String, Object>> attrCollection) {
        Map<String, Object> result = new HashMap<>();
        attrCollection.stream().flatMap(ac -> ac.entrySet().stream()).forEach(entry -> {
            Object effectiveValue = entry.getValue();
            if (result.containsKey(entry.getKey()) && effectiveValue instanceof Integer) {
                try {
                    effectiveValue = ((Integer) effectiveValue).intValue() + ((Integer) result.get(entry.getKey())).intValue();
                } catch (Exception ignore) {
                    // This is best effort only
                }
            }
            result.put(entry.getKey(), effectiveValue);
        });
        return result;
    }

    public static Map<String, Object> mergeAttributesOf(Collection<? extends ResourceConsumption> attrCollection) {
        return mergeAttributes(attrCollection.stream().map(ResourceConsumption::getAttributes).collect(Collectors.toList()));
    }

    private static <C extends ResourceConsumption> void verifySameKind(Collection<C> consumptions) {
        Preconditions.checkArgument(!consumptions.isEmpty(), "Empty argument list");

        Iterator<C> it = consumptions.iterator();
        C ref = it.next();

        if (!it.hasNext()) {
            return;
        }

        while (it.hasNext()) {
            C second = it.next();
            Preconditions.checkArgument(ref.getConsumerName().equals(second.getConsumerName()),
                    "Consumer names different %s != %s", ref.getConsumerName(), second.getConsumerName());
            Preconditions.checkArgument(ref.getConsumptionLevel().equals(second.getConsumptionLevel()),
                    "Consumption levels different %s != %s", ref.getConsumptionLevel(), second.getConsumptionLevel());
        }
    }
}
