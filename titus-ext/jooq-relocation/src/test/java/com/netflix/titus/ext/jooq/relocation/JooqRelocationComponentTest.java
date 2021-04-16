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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        properties = {
                "spring.application.name=test",
                "titus.ext.jooq.relocation.inMemoryDb=true"
        },
        classes = {
                JooqRelocationContextComponent.class,
                JooqRelocationComponent.class,
                JooqRelocationComponentTest.class,
        }
)
public class JooqRelocationComponentTest {

    @Bean
    public TitusRuntime getTitusRuntime() {
        return TitusRuntimes.internal();
    }

    @Autowired
    public TaskRelocationStore relocationStore;

    @Autowired
    public TaskRelocationResultStore resultStore;

    @Test
    public void testDependencyOnRelocationSchemaManager() {
        // Nothing to do. We test the spring initialization order here only.
    }
}
