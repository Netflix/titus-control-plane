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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitInjection;

public class DefaultFitComponent implements FitComponent {

    private final String id;

    private final ConcurrentMap<String, FitComponent> children = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FitInjection> injections = new ConcurrentHashMap<>();

    public DefaultFitComponent(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public FitComponent createChild(String childId) {
        DefaultFitComponent newChild = new DefaultFitComponent(childId);
        children.put(childId, newChild);
        return newChild;
    }

    @Override
    public List<FitComponent> getChildren() {
        return new ArrayList<>(children.values());
    }

    @Override
    public FitComponent getChild(String id) {
        FitComponent child = children.get(id);
        Preconditions.checkArgument(child != null, "FitComponent %s not found", id);
        return child;
    }

    @Override
    public Optional<FitComponent> findChild(String id) {
        return Optional.ofNullable(children.get(id));
    }

    @Override
    public FitComponent addInjection(FitInjection injection) {
        injections.put(injection.getId(), injection);
        return this;
    }

    @Override
    public List<FitInjection> getInjections() {
        return new ArrayList<>(injections.values());
    }

    @Override
    public FitInjection getInjection(String id) {
        return Preconditions.checkNotNull(injections.get(id), "Injection %s not found", id);
    }

    @Override
    public Optional<FitInjection> findInjection(String id) {
        return Optional.ofNullable(injections.get(id));
    }

    @Override
    public void acceptInjections(Consumer<FitInjection> evaluator) {
        injections.values().forEach(evaluator);
        children.values().forEach(c -> c.acceptInjections(evaluator));
    }
}
