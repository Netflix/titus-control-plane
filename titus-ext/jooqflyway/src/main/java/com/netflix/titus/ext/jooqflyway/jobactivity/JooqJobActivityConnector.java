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
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;


@Configuration
public class JooqJobActivityConnector {

    @Bean
    public JooqConfiguration getJooqConfiguration(Environment environment) {
        return new JooqConfigurationBean(environment);
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    public DataSource getDataSource(JooqConfiguration configuration) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setJdbcUrl(configuration.getJdbcUrl());
        hikariConfig.setUsername(configuration.getJdbcUsername());
        hikariConfig.setSchema(configuration.getJdbcSchema());
        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setRegisterMbeans(true);
        hikariConfig.setLeakDetectionThreshold(3000);
        hikariConfig.setPassword(configuration.getJdbcPassword());

        return new HikariDataSource(hikariConfig);
    }

    @Bean
    @Qualifier("jobActivityDSLContext")
    public DSLContext getJobActivityDSLContext(JooqConfiguration configuration) {
        DSLContext dslContext = DSL.using(getDataSource(configuration), dialect());
        dslContext.settings().setQueryTimeout(60);
        return dslContext;
    }

    @Bean
    @Qualifier("producerDSLContext")
    public DSLContext getProducerDSLContext(JooqConfiguration configuration) {
        ConnectionProvider connectionProvider;
        try {
            connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection(configuration.getProducerDatatabaseUrl()));
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize connection to Postgres database", e);
        }
        return new DefaultDSLContext(connectionProvider, SQLDialect.POSTGRES_9_5);
    }


}
