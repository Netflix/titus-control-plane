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

package io.netflix.titus.master;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;
import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.netflix.titus.common.util.guice.ContainerEventBus;
import io.netflix.titus.master.zookeeper.ZookeeperModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitusMaster {

    private static final Logger logger = LoggerFactory.getLogger(TitusMaster.class);

    private CountDownLatch blockUntilShutdown = new CountDownLatch(1);
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

    public TitusMaster() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        };
        t.setDaemon(true);
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(t);
    }

    public void start() throws Exception {
        logger.info("Starting Titus Master");
        try {
            blockUntilShutdown.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        if (shutdownInitiated.compareAndSet(false, true)) {
            logger.info("Shutting down Titus Master");
            blockUntilShutdown.countDown();
        } else {
            logger.info("Shutdown already initiated, not starting again");
        }
    }

    @Argument(alias = "p", description = "Specify a configuration file")
    private static String propFile;

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
                                    .put("governator.jetty.embedded.port", "${mantis.master.apiport}")
                                    .put("governator.jetty.embedded.webAppResourceBase", resourceDir)
                                    .build());
                            bindApplicationConfigurationOverride().toInstance(loadPropertiesFile(propFile));
                        }
                    }
            ).createInjector();
            injector.getInstance(ContainerEventBus.class).submitInOrder(new ContainerEventBus.ContainerStartedEvent());

            TitusMaster master = new TitusMaster();
            master.start(); // blocks until shutdown hook (ctrl-c)

            injector.close();
        } catch (Exception e) {
            // unexpected to get a RuntimeException, will exit
            logger.error("Unexpected error: " + e.getMessage(), e);
            System.exit(2);
        }
    }

    private static MapConfig loadPropertiesFile(String propFile) {
        if (propFile == null) {
            return MapConfig.from(Collections.emptyMap());
        }
        Properties props = new Properties();
        try (FileReader fr = new FileReader(propFile)) {
            props.load(fr);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load file " + propFile, e);
        }
        return MapConfig.from(props);
    }
}