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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.federation.EmbeddedTitusFederation;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import org.junit.rules.ExternalResource;

public class TitusStackResource extends ExternalResource {

    public static String V3_ENGINE_APP_PREFIX = "v3App";

    private final List<EmbeddedTitusCell> embeddedTitusCells;
    private final Optional<EmbeddedTitusFederation> federation;

    public TitusStackResource(EmbeddedTitusCell embeddedTitusCell, boolean federationEnabled) {
        if (federationEnabled) {
            this.embeddedTitusCells = Collections.singletonList(embeddedTitusCell);
            this.federation = Optional.of(EmbeddedTitusFederation.aDefaultTitusFederation().withCell(".*", embeddedTitusCell).build());
        } else {
            this.embeddedTitusCells = Collections.singletonList(embeddedTitusCell);
            this.federation = Optional.empty();
        }
    }

    public TitusStackResource(EmbeddedTitusCell embeddedTitusCell) {
        this(embeddedTitusCell, "true".equalsIgnoreCase(System.getProperty("titus.test.federation", "true")));
    }

    public TitusStackResource(EmbeddedTitusFederation federation) {
        this.embeddedTitusCells = federation.getCells();
        this.federation = Optional.of(federation);
    }

    @Override
    public void before() {
        embeddedTitusCells.forEach(EmbeddedTitusCell::boot);
        federation.ifPresent(EmbeddedTitusFederation::boot);
    }

    @Override
    public void after() {
        federation.ifPresent(EmbeddedTitusFederation::shutdown);
        embeddedTitusCells.forEach(EmbeddedTitusCell::shutdown);
    }

    public EmbeddedTitusMaster getMaster() {
        Preconditions.checkState(embeddedTitusCells.size() == 1, "Multiple TitusMasters");
        return embeddedTitusCells.get(0).getMaster();
    }

    public EmbeddedTitusGateway getGateway() {
        Preconditions.checkState(embeddedTitusCells.size() == 1, "Multiple TitusGateways");
        return embeddedTitusCells.get(0).getGateway();
    }

    public EmbeddedTitusOperations getOperations() {
        return federation.map(EmbeddedTitusFederation::getTitusOperations).orElse(embeddedTitusCells.get(0).getTitusOperations());
    }

    public EmbeddedKubeCluster getEmbeddedKubeCluster(int cellIdx) {
        return embeddedTitusCells.get(cellIdx).getEmbeddedKubeCluster();
    }
}
