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

package com.netflix.titus.ext.jooq.relocation;

import javax.inject.Singleton;

import com.netflix.titus.ext.jooq.JooqConfiguration;
import com.netflix.titus.ext.jooq.JooqContext;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RelocationSchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(RelocationSchemaManager.class);

    public RelocationSchemaManager(JooqConfiguration configuration, JooqContext jooqContext) {
        if (configuration.isCreateSchemaIfNotExist()) {
            logger.info("Creating/migrating task relocation DB schema...");
            Flyway flyway = Flyway.configure()
                    .schemas("relocation")
                    .locations("classpath:db/migration/relocation")
                    .dataSource(jooqContext.getDataSource())
                    .load();
            flyway.migrate();
        }
    }
}
