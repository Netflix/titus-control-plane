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

package io.netflix.titus.common.util.guice.internal;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.guice.ActivationLifecycle;
import io.netflix.titus.common.util.guice.ContainerEventBusModule;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.Deactivator;
import io.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ActivationProvisionListenerTest {

    private final List<Pair<String, String>> activationTrace = new ArrayList<>();

    @Test
    public void testActivationLifecycle() throws Exception {
        LifecycleInjector injector = InjectorBuilder.fromModules(
                new ContainerEventBusModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Registry.class).toInstance(new DefaultRegistry());
                        bind(ActivationProvisionListenerTest.class).toInstance(ActivationProvisionListenerTest.this);
                        bind(ServiceA.class).asEagerSingleton();
                    }
                }).createInjector();

        ActivationLifecycle activationLifecycle = injector.getInstance(ActivationLifecycle.class);
        ServiceA serviceA = injector.getInstance(ServiceA.class);

        assertThat(serviceA.state).isEqualTo("NOT_READY");

        activationLifecycle.activate();
        assertThat(activationLifecycle.isActive(serviceA)).isTrue();
        assertThat(serviceA.state).isEqualTo("ACTIVATED");

        activationLifecycle.deactivate();
        assertThat(activationLifecycle.isActive(serviceA)).isFalse();
        assertThat(serviceA.state).isEqualTo("DEACTIVATED");
    }

    @Test
    public void testServiceReordering() throws Exception {
        LifecycleInjector injector = InjectorBuilder.fromModules(
                new ContainerEventBusModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Registry.class).toInstance(new DefaultRegistry());
                        bind(ActivationProvisionListenerTest.class).toInstance(ActivationProvisionListenerTest.this);
                        bind(ServiceB.class).asEagerSingleton();
                        bind(ServiceA.class).asEagerSingleton();
                    }
                }).createInjector();

        ActivationLifecycle activationLifecycle = injector.getInstance(ActivationLifecycle.class);
        activationLifecycle.activate();

        assertThat(activationTrace).containsExactlyInAnyOrder(Pair.of("serviceA", "ACTIVATED"), Pair.of("serviceB", "ACTIVATED"));
    }

    @Singleton
    static class ServiceA {

        private final ActivationProvisionListenerTest owner;

        String state = "NOT_READY";

        @Inject
        public ServiceA(ActivationProvisionListenerTest owner) {
            this.owner = owner;
        }

        @Activator
        public void activate() {
            state = "ACTIVATED";
            owner.activationTrace.add(Pair.of("serviceA", state));
        }

        @Deactivator
        public void deactivate() {
            state = "DEACTIVATED";
            owner.activationTrace.add(Pair.of("serviceA", state));
        }
    }

    @Singleton
    static class ServiceB {

        private final ActivationProvisionListenerTest owner;

        String state = "NOT_READY";

        @Inject
        public ServiceB(ActivationProvisionListenerTest owner) {
            this.owner = owner;
        }

        @Activator(after = ServiceA.class)
        public void activate() {
            state = "ACTIVATED";
            owner.activationTrace.add(Pair.of("serviceB", state));
        }

        @Deactivator
        public void deactivate() {
            state = "DEACTIVATED";
            owner.activationTrace.add(Pair.of("serviceB", state));
        }
    }
}