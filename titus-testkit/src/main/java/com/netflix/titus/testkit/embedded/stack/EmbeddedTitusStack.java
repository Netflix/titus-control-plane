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

package com.netflix.titus.testkit.embedded.stack;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;

/**
 * Represents complete Titus stack, which includes master, gateway and agents.
 */
public class EmbeddedTitusStack {

    private final EmbeddedTitusMaster master;
    private final EmbeddedTitusGateway gateway;
    private final EmbeddedTitusOperations titusOperations;

    private EmbeddedTitusStack(EmbeddedTitusMaster master, EmbeddedTitusGateway gateway) {
        this.master = master;
        this.gateway = gateway;
        this.titusOperations = new EmbeddedTitusOperations(master, gateway);
    }

    public EmbeddedTitusStack toMaster(Function<EmbeddedTitusMaster.Builder, EmbeddedTitusMaster.Builder> masterTransformer) {
        return new EmbeddedTitusStack(masterTransformer.apply(master.toBuilder()).build(), gateway);
    }

    public EmbeddedTitusStack boot() {
        master.boot();
        gateway.boot();
        return this;
    }

    public EmbeddedTitusStack shutdown() {
        master.shutdown();
        gateway.shutdown();
        return this;
    }

    public EmbeddedTitusMaster getMaster() {
        return master;
    }

    public EmbeddedTitusGateway getGateway() {
        return gateway;
    }

    public EmbeddedTitusOperations getTitusOperations() {
        return titusOperations;
    }

    public static EmbeddedTitusStack.Builder aTitusStack() {
        return new Builder();
    }

    public static class Builder {

        private EmbeddedTitusMaster master;
        private EmbeddedTitusGateway gateway;
        private boolean defaultGateway;

        public Builder withMaster(EmbeddedTitusMaster master) {
            this.master = master;
            return this;
        }

        public Builder withGateway(EmbeddedTitusGateway gateway) {
            this.gateway = gateway;
            return this;
        }

        public Builder withDefaultGateway() {
            this.defaultGateway = true;
            return this;
        }

        public EmbeddedTitusStack build() {
            Preconditions.checkNotNull(master, "TitusMaster not set");
            Preconditions.checkState(gateway != null || defaultGateway, "TitusGateway not set, nor default gateway requested");

            if (defaultGateway) {
                gateway = EmbeddedTitusGateway.aDefaultTitusGateway()
                        .withMasterEndpoint("localhost", master.getGrpcPort(), master.getApiPort())
                        .withStore(master.getJobStore())
                        .build();
            } else {
                gateway = gateway.toBuilder()
                        .withMasterEndpoint("localhost", master.getGrpcPort(), master.getApiPort())
                        .withStore(master.getJobStore())
                        .build();
            }

            return new EmbeddedTitusStack(master, gateway);
        }
    }
}
