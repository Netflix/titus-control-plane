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

package com.netflix.titus.testkit.junit.jaxrs;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.ws.rs.core.Application;

import com.netflix.titus.common.network.socket.UnusedSocketPortAllocator;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.rules.ExternalResource;

public class JaxRsServerResource<S> extends ExternalResource {

    private final S restService;
    private final List<Filter> filters;
    private final List<Object> providers;

    private Server server;
    private ExecutorService executor;
    private int port;
    private String baseURI;

    private JaxRsServerResource(S restService, List<Filter> filters, List<Object> providers) {
        this.restService = restService;
        this.filters = filters;
        this.providers = providers;
    }

    @Override
    protected void before() throws Throwable {
        Application application = new Application() {
            @Override
            public Set<Object> getSingletons() {
                Set<Object> result = new HashSet<>();
                result.addAll(providers);
                result.add(restService);
                return result;
            }
        };

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        filters.forEach(filter -> context.addFilter(
                new FilterHolder(filter),
                "/*",
                EnumSet.of(DispatcherType.REQUEST)
        ));
        context.addServlet(new ServletHolder(new ServletContainer(application)), "/*");

        this.port = UnusedSocketPortAllocator.global().allocate();
        server = new Server(port);
        server.setHandler(context);

        baseURI = "http://localhost:" + port;

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "jettyServer");
            t.setDaemon(true);
            return t;
        });

        executor.execute(() -> {
            try {
                server.start();
                server.join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // We cannot depend on Jetty running state, hence the active polling of the REST endpoint
        int responseCode = -1;
        do {
            try {
                HttpURLConnection connection = (HttpURLConnection) new URL(baseURI + "/badEndpoint").openConnection();
                responseCode = connection.getResponseCode();
            } catch (IOException ignore) {
                Thread.sleep(10);
            }
        } while (responseCode != 404);
    }

    @Override
    protected void after() {
        if (server != null && executor != null) {
            try {
                server.stop();
            } catch (Exception ignore) {
            }
            executor.shutdownNow();
        }
    }

    public static <S> Builder<S> newBuilder(S restService) {
        return new Builder<>(restService);
    }

    public int getPort() {
        return port;
    }

    public String getBaseURI() {
        return baseURI;
    }

    public static class Builder<S> {

        private final S restService;
        private final List<Filter> filters = new ArrayList<>();
        private final List<Object> providers = new ArrayList<>();

        private Builder(S restService) {
            this.restService = restService;
        }

        public Builder<S> withFilter(Filter filter) {
            filters.add(filter);
            return this;
        }

        public Builder<S> withProvider(Object provider) {
            providers.add(provider);
            return this;
        }

        public Builder<S> withProviders(Object... providers) {
            for (Object provider : providers) {
                this.providers.add(provider);
            }
            return this;
        }

        public JaxRsServerResource<S> build() {
            return new JaxRsServerResource<>(restService, filters, providers);
        }
    }
}
