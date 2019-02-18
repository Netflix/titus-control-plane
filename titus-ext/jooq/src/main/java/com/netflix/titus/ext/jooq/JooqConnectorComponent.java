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

import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConnectionProvider;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class JooqConnectorComponent {

    @Bean
    public JooqConfiguration getJooqConfiguration(Environment environment) {
        return new JooqConfigurationBean(environment);
    }

    @Bean
    public DSLContext getDSLContext(JooqConfiguration configuration) {
        ConnectionProvider connectionProvider;
        try {
            connectionProvider = new DefaultConnectionProvider(DriverManager.getConnection(configuration.getDatabaseUrl()));
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize connection to Postgres database", e);
        }
        return new DefaultDSLContext(connectionProvider, SQLDialect.POSTGRES_9_5);
    }
}
