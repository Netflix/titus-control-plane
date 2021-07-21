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

package com.netflix.titus.ext.jooq.embedded;

import javax.sql.DataSource;

import com.netflix.titus.ext.jooq.JooqContext;
import com.netflix.titus.ext.jooq.profile.DatabaseProfile;
import com.netflix.titus.ext.jooq.profile.DatabaseProfileLoader;
import com.netflix.titus.ext.jooq.profile.DatabaseProfiles;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;

public class EmbeddedJooqContext implements JooqContext {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJooqContext.class);

    private static final SQLDialect DEFAULT_DIALECT = SQLDialect.POSTGRES;

    private final DataSource dataSource;
    private final EmbeddedPostgresService embeddedPostgresService;
    private final DefaultDSLContext dslContext;

    public EmbeddedJooqContext(ConfigurableApplicationContext context, String profileName) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setAutoCommit(true);

        DatabaseProfiles profiles = DatabaseProfileLoader.loadLocal();
        logger.info("Loaded embedded database profiles: {}", profiles);

        DatabaseProfile profile = profiles.getProfiles().get(profileName);
        if (profile != null) {
            logger.info("Using embedded database profile: {}", profile);
            hikariConfig.setJdbcUrl(profile.getDatabaseUrl());
            hikariConfig.setUsername(profile.getUser());
            hikariConfig.setPassword(profile.getPassword());
            this.embeddedPostgresService = null;
        } else {
            // No valid profile. Start embeddedPostgresService
            logger.info("No embedded database profile found. Starting embedded Postgres service");
            EmbeddedPostgresService embeddedPostgresService;
            try {
                embeddedPostgresService = context.getBean(EmbeddedPostgresService.class);
            } catch (Exception e) {
                ((GenericWebApplicationContext) context).registerBean(EmbeddedPostgresService.class);
                embeddedPostgresService = context.getBean(EmbeddedPostgresService.class);
            }
            this.embeddedPostgresService = embeddedPostgresService;
            //((GenericWebApplicationContext) context).registerBean();
            hikariConfig.setDataSource(this.embeddedPostgresService.getDataSource());
        }

        // Connection management
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setLeakDetectionThreshold(3000);

        this.dataSource = new HikariDataSource(hikariConfig);
        this.dslContext = new DefaultDSLContext(dataSource, DEFAULT_DIALECT);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DefaultDSLContext getDslContext() {
        return dslContext;
    }
}
