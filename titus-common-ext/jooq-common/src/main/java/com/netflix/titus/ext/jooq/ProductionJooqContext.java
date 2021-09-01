/*
 * Copyright 2021 Netflix, Inc.
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

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.ext.jooq.spectator.SpectatorHikariMetricsTrackerFactory;
import com.netflix.titus.ext.jooq.spectator.SpectatorJooqExecuteListener;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Configuration;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.postgresql.PGProperty;

@Singleton
public class ProductionJooqContext implements JooqContext {

    private final DataSource dataSource;
    private final DefaultDSLContext dslContext;

    @Inject
    public ProductionJooqContext(JooqConfiguration jooqConfiguration, TitusRuntime titusRuntime) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setAutoCommit(true);
        hikariConfig.setMetricsTrackerFactory(new SpectatorHikariMetricsTrackerFactory(
                titusRuntime.getRegistry().createId("titus.jooq.hikari"),
                titusRuntime
        ));

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);
        hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
        hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
        hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
        hikariConfig.setJdbcUrl(jooqConfiguration.getDatabaseUrl());

        this.dataSource = new HikariDataSource(hikariConfig);
        Configuration config = new DefaultConfiguration()
                .derive(dataSource)
                .derive(DEFAULT_DIALECT)
                .derive(new SpectatorJooqExecuteListener(
                        titusRuntime.getRegistry().createId("titus.jooq.executorListener"),
                        titusRuntime
                ));
        if (jooqConfiguration.isOwnExecutorPool()) {
            config = config.derive(ExecutorsExt.instrumentedFixedSizeThreadPool(titusRuntime.getRegistry(), "jooq", jooqConfiguration.getExecutorPoolSize()));
        }
        this.dslContext = new DefaultDSLContext(config);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DefaultDSLContext getDslContext() {
        return dslContext;
    }
}
