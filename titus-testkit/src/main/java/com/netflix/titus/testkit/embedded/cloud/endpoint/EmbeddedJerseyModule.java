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

package com.netflix.titus.testkit.embedded.cloud.endpoint;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.embedded.cloud.endpoint.rest.SimulatedAgentsServiceResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public final class EmbeddedJerseyModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(EmbeddedServer.class).asEagerSingleton();
    }

    @Singleton
    public static class EmbeddedServer {

        private final Server jettyServer;

        @Inject
        public EmbeddedServer(SimulatedCloudGateway gateway) {
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            jettyServer = new Server(8099);
            jettyServer.setHandler(context);

            // Static content

            DefaultServlet staticContentServlet = new DefaultServlet();
            ServletHolder holder = new ServletHolder(staticContentServlet);
            holder.setInitOrder(1);
            context.addServlet(holder, "/static/*");
            context.setResourceBase(resolveStaticContentLocation());

            // Jersey

            DefaultResourceConfig config = new DefaultResourceConfig();
            config.getClasses().add(JsonMessageReaderWriter.class);
            config.getClasses().add(TitusExceptionMapper.class);
            config.getSingletons().add(new SimulatedAgentsServiceResource(gateway));

            ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(config));
            jerseyServlet.setInitOrder(0);
            context.addServlet(jerseyServlet, "/cloud/*");
        }

        @PostConstruct
        public void start() {
            try {
                jettyServer.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @PreDestroy
        public void shutdown() {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private static String resolveStaticContentLocation() {
        URL resource = EmbeddedJerseyModule.class.getClassLoader().getResource("embedded/cloud/static/index.html");
        if (resource == null) {
            return "/";
        }
        if (resource.getProtocol().equals("file")) {
            try {
                return new File(resource.toURI()).getParentFile().getParent();
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
        }
        // TODO Handle jar content properly.
        return "/";
    }
}
