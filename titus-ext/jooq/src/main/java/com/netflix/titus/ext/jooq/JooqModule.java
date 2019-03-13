/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.jooq;

import java.sql.DriverManager;
import java.sql.SQLException;
import javax.inject.Inject;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.archaius.ConfigProxyFactory;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultDSLContext;

public class JooqModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DSLContext.class).toProvider(DefaultDSLContextProvider.class);
    }

    @Provides
    @Singleton
    public JooqConfiguration getJooqConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JooqConfiguration.class);
    }

    static class DefaultDSLContextProvider implements Provider<DSLContext> {

        ConnectionProvider connectionProvider;
        SQLDialect dialect;

        @Inject
        DefaultDSLContextProvider(JooqConfiguration configuration) {
            try {
                if (configuration.isInMemoryDb()) {
                    connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection("jdbc:hsqldb:mem:junit" + System.currentTimeMillis(), "SA", ""));
                    dialect = SQLDialect.HSQLDB;
                } else {
                    connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection(configuration.getDatabaseUrl()));
                    dialect = SQLDialect.POSTGRES_9_5;
                }

                // Explicitly set autoCommit as jOOQ Store impls should expect automatic transaction management
                ((DefaultConnectionProvider) connectionProvider).setAutoCommit(true);
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot initialize connection to Postgres database", e);
            }
        }

        @Override
        public DSLContext get() {
            return new DefaultDSLContext(connectionProvider, dialect);
        }
    }
}
