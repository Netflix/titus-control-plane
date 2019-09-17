package com.netflix.titus.ext.jooqflyway.jobactivity;

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

        @Inject
        DefaultDSLContextProvider(JooqConfiguration configuration) {
            try {
                connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection(configuration.getDatabaseUrl()));
                // Explicitly set autoCommit as jOOQ Store impls should expect automatic transaction management
                ((DefaultConnectionProvider) connectionProvider).setAutoCommit(true);
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot initialize connection to Postgres database " + configuration.getDatabaseUrl(), e);
            }
        }
        @Override
        public DSLContext get() {
            return new DefaultDSLContext(connectionProvider, SQLDialect.POSTGRES);
        }
    }
}
