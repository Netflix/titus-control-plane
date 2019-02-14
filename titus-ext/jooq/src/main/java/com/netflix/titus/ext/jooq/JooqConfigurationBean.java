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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.util.SpringConfigurationUtil;
import org.springframework.core.env.Environment;

@Singleton
public class JooqConfigurationBean implements JooqConfiguration {

    private static final String PREFIX = "titus.ext.jooq.";

    private final Environment environment;

    @Inject
    public JooqConfigurationBean(Environment environment) {
        this.environment = environment;
    }

    @Override
    public String getDatabaseUrl() {
        return SpringConfigurationUtil.getString(environment, PREFIX + "databaseUrl", "jdbc://notSet");
    }
}
