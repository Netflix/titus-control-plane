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

import java.util.Optional;

import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.federation.EmbeddedTitusFederation;
import org.junit.rules.ExternalResource;

public class TitusStackResource extends ExternalResource {

    public static String V2_ENGINE_APP_PREFIX = "v2App";
    public static String V3_ENGINE_APP_PREFIX = "v3App";

    private final EmbeddedTitusCell embeddedTitusCell;
    private final Optional<EmbeddedTitusFederation> federation;

    public TitusStackResource(EmbeddedTitusCell embeddedTitusCell, boolean federationEnabled) {
        if (federationEnabled) {
            this.embeddedTitusCell = embeddedTitusCell;
            this.federation = Optional.of(EmbeddedTitusFederation.aDefaultTitusFederation().withCell("defaultCell", ".*", embeddedTitusCell).build());
        } else {
            this.embeddedTitusCell = embeddedTitusCell;
            this.federation = Optional.empty();
        }
    }

    public TitusStackResource(EmbeddedTitusCell embeddedTitusCell) {
        this(embeddedTitusCell, "true".equalsIgnoreCase(System.getProperty("titus.test.federation", "true")));
    }

    @Override
    public void before() {
        embeddedTitusCell.boot();
        federation.ifPresent(EmbeddedTitusFederation::boot);
    }

    @Override
    public void after() {
        federation.ifPresent(EmbeddedTitusFederation::shutdown);
        embeddedTitusCell.shutdown();
    }

    public EmbeddedTitusMaster getMaster() {
        return embeddedTitusCell.getMaster();
    }

    public EmbeddedTitusGateway getGateway() {
        return embeddedTitusCell.getGateway();
    }

    public EmbeddedTitusOperations getOperations() {
        return federation.map(EmbeddedTitusFederation::getTitusOperations).orElse(embeddedTitusCell.getTitusOperations());
    }
}
