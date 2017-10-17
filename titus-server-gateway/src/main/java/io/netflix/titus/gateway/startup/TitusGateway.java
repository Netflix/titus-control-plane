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

package io.netflix.titus.gateway.startup;

import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;

/**
 * The "main" class that boots up the service. When it's deployed within a servlet container such
 * as Tomcat, only the createInjector() is called. For local testing one simply invokes the
 * main() getMethod as if running a normal Java app.
 */
public class TitusGateway {

    public static void main(String[] args) throws Exception {
        InjectorBuilder.fromModules(
                new TitusGatewayModule(),
                new Archaius2JettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindDefaultConfig().toInstance(MapConfig.builder()
                                .build());
                        bindApplicationConfigurationOverrideResource("laptop");
                    }
                }).createInjector().awaitTermination();
    }
}
