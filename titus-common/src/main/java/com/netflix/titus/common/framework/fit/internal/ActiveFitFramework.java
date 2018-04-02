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


import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitRegistry;

public class ActiveFitFramework extends FitFramework {

    private static final String ROOT_COMPONENT = "root";

    private final FitRegistry fitRegistry;

    private final DefaultFitComponent rootComponent;

    public ActiveFitFramework(FitRegistry fitRegistry) {
        this.fitRegistry = fitRegistry;
        this.rootComponent = new DefaultFitComponent(ROOT_COMPONENT);
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public FitRegistry getFitRegistry() {
        return fitRegistry;
    }

    @Override
    public FitComponent getRootComponent() {
        return rootComponent;
    }

    @Override
    public FitInjection.Builder newFitInjectionBuilder(String id) {
        return DefaultFitInjection.newBuilder(id);
    }

    @Override
    public <I> I newFitProxy(I interf, FitInjection injection) {
        return FitInvocationHandler.newProxy(interf, injection);
    }
}
