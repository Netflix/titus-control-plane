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

package com.netflix.titus.master;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;
import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.guice.ContainerEventBus;
import com.netflix.titus.master.zookeeper.ZookeeperModule;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitusMaster {
    private static final Logger logger = LoggerFactory.getLogger(TitusMaster.class);

    @Argument(alias = "p", description = "Specify a properties file", required = true)
    private static String propertiesFile;

    public static void main(String[] args) {
        try {
            Args.parse(TitusMaster.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(TitusMaster.class);
            System.exit(1);
        }

        try {
            String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();

            LifecycleInjector injector = InjectorBuilder.fromModules(
                    new ZookeeperModule(),
                    Modules.override(new TitusRuntimeModule()).with(
                            new AbstractModule() {
                                @Override
                                protected void configure() {
                                    bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
                                    bind(Registry.class).toInstance(new DefaultRegistry());
                                }
                            }
                    ),
                    new TitusMasterModule(),
                    new Archaius2JettyModule(),
                    new ArchaiusModule() {
                        @Override
                        protected void configureArchaius() {
                            bindDefaultConfig().toInstance(MapConfig.builder()
                                    .put("governator.jetty.embedded.port", "${titus.master.apiport}")
                                    .put("governator.jetty.embedded.webAppResourceBase", resourceDir)
                                    .build());
                            bindApplicationConfigurationOverride().toInstance(loadPropertiesFile(propertiesFile));
                        }
                    }
            ).createInjector();
            injector.getInstance(ContainerEventBus.class).submitInOrder(new ContainerEventBus.ContainerStartedEvent());
            injector.awaitTermination();
        } catch (Exception e) {
            // unexpected to get a RuntimeException, will exit
            logger.error("Unexpected error: {}", e.getMessage(), e);
            System.exit(2);
        }
    }

    private static MapConfig loadPropertiesFile(String propertiesFile) {
        if (propertiesFile == null) {
            return MapConfig.from(Collections.emptyMap());
        }
        Properties properties = new Properties();
        try (FileReader fr = new FileReader(propertiesFile)) {
            properties.load(fr);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load file: " + propertiesFile, e);
        }
        return MapConfig.from(properties);
    }
}