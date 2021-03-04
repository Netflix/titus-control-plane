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

package com.netflix.titus.ext.jooqflyway.jobactivity;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.postgresql.PGProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JooqJobActivityConnectorComponent {

    @Bean
    public JooqConfiguration getJooqPropertyConfiguration() {
        return new JooqConfiguration();
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    public EmbeddedPostgresService getEmbeddedPostgresService(JooqConfiguration jooqConfiguration) {
        return new EmbeddedPostgresService(jooqConfiguration);
    }

    @Bean
    @Primary
    @Qualifier("jobActivityJooqContext")
    public JooqContext getJobActivityJooqContext(JooqConfiguration jooqConfiguration, EmbeddedPostgresService embeddedPostgresService) {
        HikariConfig hikariConfig = new HikariConfig();
        System.out.println("CONSUMER");

        hikariConfig.setAutoCommit(true);

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);
        if (jooqConfiguration.isInMemoryDb()) {
            hikariConfig.setDataSource(embeddedPostgresService.getDataSource());
            return new JooqContext(jooqConfiguration, embeddedPostgresService.getDataSource(), embeddedPostgresService);
        } else {
            hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
            hikariConfig.setJdbcUrl(jooqConfiguration.getDatabaseUrl());
            HikariDataSource dataSource = new HikariDataSource(hikariConfig);

            return new JooqContext(jooqConfiguration, dataSource, embeddedPostgresService);
        }
    }

    @Bean
    @Qualifier("producerJooqContext")
    public JooqContext getJooqProducerContext(JooqConfiguration jooqConfiguration, EmbeddedPostgresService producerEmbeddedPostgresService) {
        HikariConfig hikariConfig = new HikariConfig();
        System.out.println("PRODUCER");
        hikariConfig.setAutoCommit(true);

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);

        if (jooqConfiguration.isInMemoryDb()) {
            hikariConfig.setDataSource(producerEmbeddedPostgresService.getDataSource());
        } else {
            hikariConfig.addDataSourceProperty(PGProperty.SSL.getName(), "true");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_MODE.getName(), "verify-ca");
            hikariConfig.addDataSourceProperty(PGProperty.SSL_FACTORY.getName(), RDSSSLSocketFactory.class.getName());
            hikariConfig.setJdbcUrl(jooqConfiguration.getProducerDatatabaseUrl());
        }

        return new JooqContext(jooqConfiguration, new HikariDataSource(hikariConfig), producerEmbeddedPostgresService);
    }

    @Bean
    @Primary
    @Qualifier("jobActivityDslContext")
    public DSLContext getJobActivityDSLContext(JooqContext jooqJobActivityContext) {
        return jooqJobActivityContext.getDslContext();
    }


    @Bean
    @Qualifier("producerDslContext")
    public DSLContext getProducerDSLContext(JooqContext jooqProducerContext) {
        return jooqProducerContext.getDslContext();
    }

}
