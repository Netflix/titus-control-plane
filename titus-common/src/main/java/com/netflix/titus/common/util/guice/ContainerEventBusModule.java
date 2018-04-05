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

package com.netflix.titus.common.util.guice;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.guice.internal.ActivationProvisionListener;
import com.netflix.titus.common.util.guice.internal.DefaultContainerEventBus;
import com.netflix.titus.common.util.guice.internal.ProxyMethodInterceptor;

/**
 */
public final class ContainerEventBusModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ContainerEventBus.class).to(DefaultContainerEventBus.class);

        // Activation lifecycle
        ActivationProvisionListener activationProvisionListener = new ActivationProvisionListener();
        bind(ActivationLifecycle.class).toInstance(activationProvisionListener);
        bindListener(Matchers.any(), activationProvisionListener);

        // Proxies
        bindInterceptor(
                Matchers.annotatedWith(ProxyConfiguration.class),
                Matchers.any(),
                new ProxyMethodInterceptor(getProvider(ActivationLifecycle.class), getProvider(Registry.class))
        );
    }

    @Override
    public boolean equals(Object obj) {
        return getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return "ContainerEventBusModule[]";
    }
}
