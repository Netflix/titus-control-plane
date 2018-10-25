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

package com.netflix.titus.supplementary.relocation.integration;

import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import org.junit.rules.ExternalResource;

public class TaskRelocationResourceSandbox extends ExternalResource {

    private final RelocationConnectorStubs relocationConnectorStubs;

    private TaskRelocationSandbox taskRelocationSandbox;

    public TaskRelocationResourceSandbox(RelocationConnectorStubs relocationConnectorStubs) {
        this.relocationConnectorStubs = relocationConnectorStubs;
    }

    @Override
    protected void before() {
        this.taskRelocationSandbox = new TaskRelocationSandbox(relocationConnectorStubs);
    }

    public TaskRelocationSandbox getTaskRelocationSandbox() {
        return taskRelocationSandbox;
    }

    @Override
    protected void after() {
        if(taskRelocationSandbox != null) {
            try {
                taskRelocationSandbox.shutdown();
            } catch (Exception ignore) {
            }
        }
    }
}
