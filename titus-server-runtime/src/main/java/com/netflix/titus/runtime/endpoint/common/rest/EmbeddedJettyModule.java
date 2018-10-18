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

package com.netflix.titus.runtime.endpoint.common.rest;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jetty.Archaius2JettyConfig;
import com.netflix.governator.guice.jetty.JettyConfig;
import com.netflix.governator.guice.jetty.JettyModule;

public class EmbeddedJettyModule extends AbstractModule {

    private final int httpPort;

    public EmbeddedJettyModule(int httpPort) {
        this.httpPort = httpPort;
    }

    @Override
    protected void configure() {
        install(new JettyModule());
    }

    @Provides
    @Singleton
    public JettyConfig jettyConfig(ConfigProxyFactory configProxyFactory) {
        Archaius2JettyConfig config = configProxyFactory.newProxy(Archaius2JettyConfig.class);
        return new JettyConfig() {

            @Override
            public int getPort() {
                return httpPort;
            }

            @Override
            public String getResourceBase() {
                return config.getResourceBase();
            }

            @Override
            public String getWebAppResourceBase() {
                return config.getWebAppResourceBase();
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        return EmbeddedJettyModule.class.equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return EmbeddedJettyModule.class.hashCode();
    }
}
