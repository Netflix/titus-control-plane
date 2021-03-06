/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.store.memory;

import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStoreActivator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "titus.relocation.store.inMemory.enabled", havingValue = "true", matchIfMissing = true)
public class InMemoryRelocationStoreComponent {

    @Bean
    public TaskRelocationStore getTaskRelocationStore() {
        return new InMemoryTaskRelocationStore();
    }

    @Bean
    public TaskRelocationResultStore getTaskRelocationResultStore() {
        return new InMemoryTaskRelocationResultStore();
    }

    @Bean
    public TaskRelocationStoreActivator getTaskRelocationStoreActivator() {
        return new TaskRelocationStoreActivator() {
        };
    }
}
