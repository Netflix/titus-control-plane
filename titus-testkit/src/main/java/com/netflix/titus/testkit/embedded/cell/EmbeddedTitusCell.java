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

package com.netflix.titus.testkit.embedded.cell;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.runtime.endpoint.admission.AdmissionSanitizer;
import com.netflix.titus.runtime.endpoint.admission.AdmissionValidator;
import com.netflix.titus.runtime.endpoint.admission.PassJobValidator;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;

/**
 * Represents complete Titus stack, which includes master, gateway and agents.
 */
public class EmbeddedTitusCell {

    private final EmbeddedTitusMaster master;
    private final EmbeddedTitusGateway gateway;
    private final EmbeddedTitusOperations titusOperations;

    private EmbeddedTitusCell(EmbeddedTitusMaster master,
                              EmbeddedTitusGateway gateway) {
        this.master = master;
        this.gateway = gateway;
        this.titusOperations = new EmbeddedCellTitusOperations(master, gateway);
    }

    public EmbeddedTitusCell toMaster(Function<EmbeddedTitusMaster.Builder, EmbeddedTitusMaster.Builder> masterTransformer) {
        EmbeddedTitusMaster newMaster = masterTransformer.apply(master.toBuilder()).build();
        return new EmbeddedTitusCell(newMaster, gateway.toBuilder().withMaster(newMaster).build());
    }

    public EmbeddedTitusCell boot() {
        master.boot();
        gateway.boot();
        return this;
    }

    public EmbeddedTitusCell shutdown() {
        gateway.shutdown();
        master.shutdown();
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

    public static EmbeddedTitusCell.Builder aTitusCell() {
        return new Builder();
    }

    public static class Builder {

        private EmbeddedTitusMaster master;
        private EmbeddedTitusGateway gateway;
        private boolean enableREST;
        private boolean defaultGateway;
        private AdmissionValidator<JobDescriptor> validator = new PassJobValidator();
        private AdmissionSanitizer<JobDescriptor> sanitizer = new PassJobValidator();

        public Builder withMaster(EmbeddedTitusMaster master) {
            this.master = master;
            return this;
        }

        public Builder withGateway(EmbeddedTitusGateway gateway, boolean enableREST) {
            this.gateway = gateway;
            this.enableREST = enableREST;
            return this;
        }

        public Builder withDefaultGateway() {
            this.defaultGateway = true;
            return this;
        }

        public Builder withJobValidator(AdmissionValidator<JobDescriptor> validator) {
            this.validator = validator;
            return this;
        }

        public Builder withJobSanitizer(AdmissionSanitizer<JobDescriptor> sanitizer) {
            this.sanitizer = sanitizer;
            return this;
        }

        public EmbeddedTitusCell build() {
            Preconditions.checkNotNull(master, "TitusMaster not set");
            Preconditions.checkState(gateway != null || defaultGateway, "TitusGateway not set, nor default gateway requested");

            master = master.toBuilder().withEnableREST(false).build();

            if (defaultGateway) {
                gateway = EmbeddedTitusGateway.aDefaultTitusGateway()
                        .withMaster(master)
                        .withStore(master.getJobStore())
                        .withEnableREST(enableREST)
                        .withJobValidator(validator)
                        .withJobSanitizer(sanitizer)
                        .build();
            } else {
                gateway = gateway.toBuilder()
                        .withMaster(master)
                        .withStore(master.getJobStore())
                        .withEnableREST(enableREST)
                        .build();
            }

            return new EmbeddedTitusCell(master, gateway);
        }
    }
}
