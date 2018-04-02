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

package com.netflix.titus.common.framework.fit.internal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.fit.FitAction;
import com.netflix.titus.common.framework.fit.FitActionDescriptor;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitRegistry;
import com.netflix.titus.common.util.tuple.Pair;

public class DefaultFitRegistry implements FitRegistry {

    private final List<FitActionDescriptor> actionDescriptors;
    private final Map<String, BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>>> actionFactories;

    public DefaultFitRegistry(List<Pair<FitActionDescriptor, BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>>>> actions) {
        this.actionDescriptors = new CopyOnWriteArrayList<>(actions.stream().map(Pair::getLeft).collect(Collectors.toList()));
        this.actionFactories = new ConcurrentHashMap<>(actions.stream().collect(Collectors.toMap(p -> p.getLeft().getKind(), Pair::getRight)));
    }

    @Override
    public List<FitActionDescriptor> getFitActionDescriptors() {
        return actionDescriptors;
    }

    @Override
    public Function<FitInjection, FitAction> newFitActionFactory(String actionKind, String id, Map<String, String> properties) {
        return Preconditions.checkNotNull(
                actionFactories.get(actionKind), "Action kind %s not found", actionKind
        ).apply(id, properties);
    }

    @Override
    public void registerActionKind(FitActionDescriptor descriptor,
                                   BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>> factory) {
        actionFactories.put(descriptor.getKind(), factory);
        actionDescriptors.add(descriptor);
    }
}
