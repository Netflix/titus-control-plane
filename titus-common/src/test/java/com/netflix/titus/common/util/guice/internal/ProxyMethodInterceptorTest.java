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

package com.netflix.titus.common.util.guice.internal;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.guice.ActivationLifecycle;
import com.netflix.titus.common.util.guice.ContainerEventBusModule;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ProxyMethodInterceptorTest {

    @Test
    public void testInterceptionWithSimpleBinding() throws Exception {
        doTest(new AbstractModule() {
            @Override
            protected void configure() {
                bind(MyService.class).to(MyServiceImpl.class);
            }
        });
    }

    /**
     * Guice AOP works only with classes instantiated by Guice itself. This means that instances created with
     * providers, are not instrumented. This test exhibits this behavior.
     */
    @Test(expected = AssertionError.class)
    public void testInterceptionDosNotWorkWithProviders() throws Exception {
        doTest(new MyModule());
    }

    private void doTest(AbstractModule serviceModule) {
        Injector injector = Guice.createInjector(
                new ContainerEventBusModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(TitusRuntime.class).toInstance(TitusRuntimes.internal());
                    }
                },
                serviceModule
        );

        MyServiceImpl service = (MyServiceImpl) injector.getInstance(MyService.class);

        // Check that service is guarded
        assertThat(service.isActive()).isFalse();
        try {
            service.echo("hello");
            fail("Expected to fail, as service is not activated yet");
        } catch (Exception ignore) {
        }

        injector.getInstance(ActivationLifecycle.class).activate();
        assertThat(service.isActive()).isTrue();

        String reply = service.echo("hello");
        assertThat(reply).isEqualTo("HELLO");
    }

    interface MyService {
        String echo(String message);
    }

    @Singleton
    @ProxyConfiguration(types = {ProxyType.Logging, ProxyType.ActiveGuard})
    static class MyServiceImpl implements MyService {

        private boolean active;

        @Activator
        public void activate() {
            this.active = true;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public String echo(String message) {
            return message.toUpperCase();
        }
    }

    static class MyModule extends AbstractModule {
        @Override
        protected void configure() {
        }

        @Provides
        @Singleton
        public MyService getMyService() {
            return new MyServiceImpl();
        }
    }
}