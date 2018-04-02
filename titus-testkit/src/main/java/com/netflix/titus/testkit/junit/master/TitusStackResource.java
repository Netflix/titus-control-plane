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

package com.netflix.titus.testkit.junit.master;

import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack;
import org.junit.rules.ExternalResource;

public class TitusStackResource extends ExternalResource {

    public static String V2_ENGINE_APP_PREFIX = "v2App";
    public static String V3_ENGINE_APP_PREFIX = "v3App";

    private final EmbeddedTitusStack embeddedTitusStack;

    public TitusStackResource(EmbeddedTitusStack embeddedTitusStack) {
        this.embeddedTitusStack = embeddedTitusStack;
    }

    @Override
    public void before() throws Throwable {
        embeddedTitusStack.boot();
    }

    @Override
    public void after() {
        embeddedTitusStack.shutdown();
    }

    public EmbeddedTitusStack getStack() {
        return embeddedTitusStack;
    }

    public EmbeddedTitusMaster getMaster() {
        return embeddedTitusStack.getMaster();
    }

    public EmbeddedTitusGateway getGateway() {
        return embeddedTitusStack.getGateway();
    }

    public EmbeddedTitusOperations getOperations() {
        return embeddedTitusStack.getTitusOperations();
    }
}
