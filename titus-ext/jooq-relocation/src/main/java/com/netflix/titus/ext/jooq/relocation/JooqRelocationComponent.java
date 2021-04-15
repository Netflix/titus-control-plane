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

package com.netflix.titus.ext.jooq.relocation;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStoreActivator;
import org.jooq.DSLContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

@Configuration
@Import({JooqTaskRelocationGC.class})
@ConditionalOnProperty(name = "titus.ext.jooq.relocation.enabled", havingValue = "true", matchIfMissing = true)
public class JooqRelocationComponent {

    @Bean
    public JooqRelocationConfiguration getJooqRelocationConfiguration(Environment environment) {
        return Archaius2Ext.newConfiguration(JooqRelocationConfiguration.class, environment);
    }

    @Bean
    public TaskRelocationStore getTaskRelocationStore(DSLContext dslContext) {
        return new JooqTaskRelocationStore(dslContext);
    }

    @Bean
    public TaskRelocationResultStore getTaskRelocationResultStore(DSLContext dslContext,
                                                                  TitusRuntime titusRuntime) {
        return new JooqTaskRelocationResultStore(dslContext, titusRuntime);
    }

    @Bean
    public TaskRelocationStoreActivator getTaskRelocationStoreActivator(JooqTaskRelocationStore relocationStore,
                                                                        JooqTaskRelocationGC relocationGC) {
        return new TaskRelocationStoreActivator() {
            @Override
            public void activate() {
                relocationStore.activate();
                relocationGC.activate();
            }
        };
    }
}
