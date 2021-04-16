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

package com.netflix.titus.ext.jooqflyway;


import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.ext.jooq.JooqConfiguration;
import com.netflix.titus.ext.jooq.JooqContext;
import com.netflix.titus.ext.jooq.ProductionJooqContext;
import com.netflix.titus.ext.jooq.embedded.EmbeddedJooqContext;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

@Configuration
public class JooqJobActivityContextComponent {

    @Bean
    public JooqConfiguration getJooqPropertyConfiguration(Environment environment) {
        return Archaius2Ext.newConfiguration(JooqConfiguration.class, "titus.ext.supplementary.jobactivity", environment);
    }

    @Bean
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Bean
    @Primary
    @Qualifier("jobActivityJooqContext")
    public JooqContext getJobActivityJooqContext(JooqConfiguration jooqConfiguration, ConfigurableApplicationContext applicationContext) {
        if (jooqConfiguration.isInMemoryDb()) {
            return new EmbeddedJooqContext(applicationContext, "jobActivityJooqContext");
        }
        return new ProductionJooqContext(jooqConfiguration);
    }

    @Bean
    @Qualifier("producerJooqContext")
    public JooqContext getJooqProducerContext(JooqConfiguration jooqConfiguration, ConfigurableApplicationContext applicationContext) {
        if (jooqConfiguration.isInMemoryDb()) {
            return new EmbeddedJooqContext(applicationContext, "producerJooqContext");
        }
        return new ProductionJooqContext(jooqConfiguration);
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
